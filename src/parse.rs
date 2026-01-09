use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Error, Result};
use rayon::prelude::*;
use sophia::{
    api::{
        parser::{QuadParser, TripleParser},
        source::TripleSource,
    },
    iri::{relativize::Relativizer, resolve::BaseIri, Iri},
    jsonld::{JsonLdOptions, JsonLdParser},
    turtle::parser::{
        gnq::GNQuadsParser, gtrig::GTriGParser, nq::NQuadsParser, nt::NTriplesParser,
        trig::TriGParser, turtle::TurtleParser,
    },
    xml::parser::RdfXmlParser,
};

use crate::common::{
    file_or_url::FileOrUrl,
    files_or_url::{FilesOrUrl, PathOrUrl},
    format::*,
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterItem},
};
use crate::relativize::RelativizerExt;

/// Parse data in an RDF concrete syntax into quads
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// File or URL (- for stdin)
    ///
    /// To parse multiple files or URLs, use `--multiple` instead.
    /// Otherwise, defaults to stdin.
    file_or_url: Option<FileOrUrl>,

    /// Multiple filenames, glob patterns or URLs, terminated with 'm-'
    #[arg(short, long, num_args = 1.. , value_terminator = "m-", conflicts_with = "file_or_url")]
    multiple: Vec<FilesOrUrl>,

    /// Format to parse
    ///
    /// When parsing from a single source,
    /// this option overrides any format that could be guessed from the filename or HTTP headers.
    ///
    /// When parsing multiple sources (with the --multiple argument),
    /// this option is only used on files whose format can not be guessed from their extension.
    #[arg(short, long)]
    format: Option<Format>,

    /// Base IRI against which relative IRIs will be resolve
    ///
    /// If omitted, defaults to the filename/URL from which the data was loaded.
    ///
    /// Does not apply to N-Quands, N-Triples or Generalized N-Quads.
    #[arg(short, long, value_parser = |txt: &str| Iri::new(txt.to_string()))]
    base: Option<Iri<String>>,

    /// Whether to relativize parsed IRIs against the source IRI.
    ///
    /// If provided without a value, defaults to 0.
    /// If provided with a value (n≥0), represent the number of parent "directories" to also relativize.
    ///
    /// For example, `-r` will not generate any relative IRI reference starting with '..'.
    /// `-r 2` would generate relative IRI references starting with '..' or '../..',
    /// but not with `../../..`.
    #[arg(short, long)]
    relativize: Option<Option<u8>>,

    #[command(flatten)]
    options: ParserOptions,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

/// Reusable serializer options
#[derive(clap::Args, Clone, Debug)]
pub struct ParserOptions {
    /// Local cache for known contexts.
    ///
    /// Only applies to JSON-LD and YAML-LD.
    ///
    /// Every file or subdirectory `ITEM` of that directory is interpreted as a local cache for the `https://ITEM/` namespace.
    #[arg(short = 'l', long, id = "DIR", env = "DOCUMENT_LOADER_CACHE")]
    loader_local: Option<PathBuf>,

    /// Fetch unknown context IRIs as URLs.
    ///
    /// Only applies to JSON-LD and YAML-LD.
    ///
    /// This is not the default behavior,
    /// because fetching unknown contexts from the web (or the filesystem)
    /// is usually not fit for production.
    /// Consider using `--loader-local` instead.
    #[arg(short = 'u', long)]
    loader_urls: bool,
}

pub fn run(mut args: Args) -> Result<()> {
    log::trace!("parse args: {args:#?}");
    if !args.multiple.is_empty() {
        if args.base.is_some() {
            bail!("Can not use --base with --multiple (this would cause information loss)");
        }
        if args.relativize.is_some() {
            bail!("Can not use --relativize with --multiple (this would cause information loss)");
        }
    }
    let handler = QuadHandler::new(args.pipeline.take());
    if args.multiple.is_empty() {
        match args.file_or_url.take().unwrap_or(FileOrUrl::StdIn) {
            FileOrUrl::StdIn => parse_stdin(args, handler),
            FileOrUrl::File(filename) => parse_file(args, &PathBuf::from(filename), handler, false),
            FileOrUrl::Url(url) => parse_url(args, url, handler),
        }
    } else {
        let (tx, rx) = std::sync::mpsc::channel();
        let sink_thread = std::thread::spawn(|| {
            handler.handle_quads(QuadIter::new(rx.into_iter().map(QuadIterItem::Ok)))
        });
        std::mem::take(&mut args.multiple)
            .into_iter()
            .flat_map(FilesOrUrl::into_iter)
            .par_bridge()
            .for_each(|path_or_url| {
                log::debug!("{path_or_url:?}");
                let handler = QuadHandler::Sender {
                    name: path_or_url.to_string(),
                    bnode_suffix: random_bnode_suffix(),
                    tx: &tx,
                };
                if let Err(err) = match path_or_url {
                    PathOrUrl::Path(path_buf) => parse_file(args.clone(), &path_buf, handler, true),
                    PathOrUrl::Url(url) => parse_url(args.clone(), url, handler),
                } {
                    log::error!("{err}");
                }
            });
        drop(tx); // hang up the channel, so that sink_thread stops after empying it
        sink_thread.join().unwrap()
    }
}

fn parse_stdin(args: Args, handler: QuadHandler) -> std::result::Result<(), Error> {
    let format = match args.format {
        Some(f) => f,
        None => Err(Error::msg("Cannot guess format for stdin"))?,
    };
    let read = std::io::stdin();
    let base = args
        .base
        .unwrap_or_else(|| Iri::new_unchecked("x-stdin://".into()));
    let rel = make_relativizer(&base, args.relativize);
    parse_read(read, format, base, rel, args.options, handler)
}

fn parse_file(
    args: Args,
    filename: &Path,
    handler: QuadHandler,
    prefer_guess: bool,
) -> std::result::Result<(), Error> {
    let guess_format = || {
        filename
            .extension()
            .and_then(|ext| ext.to_string_lossy().parse::<Format>().ok())
    };
    let format = if prefer_guess {
        guess_format().or(args.format)
    } else {
        args.format.or_else(guess_format)
    }
    .ok_or_else(|| {
        anyhow!("Can not guess format for file {filename:?}, please specify with --format")
    })?;
    let read = std::fs::File::open(filename)?;
    let base = match args.base {
        Some(b) => b,
        None => filename_to_iri(filename)?,
    };
    let rel = make_relativizer(&base, args.relativize);
    parse_read(read, format, base, rel, args.options, handler)
}

fn parse_url(
    args: Args,
    url: reqwest::Url,
    handler: QuadHandler,
) -> std::result::Result<(), Error> {
    let base = match args.base {
        Some(b) => b,
        None => Iri::new_unchecked(url.clone().to_string()),
    };
    let rel = make_relativizer(&base, args.relativize);
    let client = reqwest::blocking::Client::new();
    let resp = client
        .get(url)
        .header("accept", ACCEPT)
        .send()?
        .error_for_status()?;
    let format = match args.format {
        Some(f) => f,
        None => match resp
            .headers()
            .get("content-type")
            .and_then(|val| val.to_str().ok())
            .and_then(|txt| txt.split(";").next())
        {
            Some(ctype) => ctype.parse(),
            None => Err(Error::msg("Cannot guess format for URL {url}")),
        }?,
    };
    parse_read(resp, format, base, rel, args.options, handler)
}

fn parse_read<R: std::io::Read>(
    read: R,
    format: Format,
    base: Iri<String>,
    mut relativizer: Option<Relativizer<String>>,
    options: ParserOptions,
    handler: QuadHandler,
) -> Result<()> {
    let bufread = BufReader::new(read);
    let quads = match format {
        GeneralizedNQuads => {
            let parser = GNQuadsParser::new();
            let quads = QuadParser::parse(&parser, bufread);
            QuadIter::from_quad_source(quads)
        }
        GeneralizedTriG => {
            let parser = GTriGParser::new()
                .with_base(Some(base.map_unchecked(Box::from).to_iri_ref().to_base()));
            let quads = QuadParser::parse(&parser, bufread);
            QuadIter::from_quad_source(quads)
        }
        JsonLd | YamlLd => {
            if options.loader_urls {
                let options = JsonLdOptions::new()
                    .with_base(base.map_unchecked(std::sync::Arc::from))
                    .with_document_loader_closure(|| {
                        sophia::jsonld::loader::ChainLoader::new(
                            make_fs_loader(options.loader_local.as_ref()),
                            sophia::jsonld::loader::ChainLoader::new(
                                sophia::jsonld::loader::FileUrlLoader::default(),
                                sophia::jsonld::loader::HttpLoader::default(),
                            ),
                        )
                    });
                parse_x_ld(format, options, bufread)?
            } else {
                let options = JsonLdOptions::new()
                    .with_base(base.map_unchecked(std::sync::Arc::from))
                    .with_document_loader_closure(|| make_fs_loader(options.loader_local.as_ref()));
                parse_x_ld(format, options, bufread)?
            }
        }
        NQuads => {
            let parser = NQuadsParser::new();
            let quads = QuadParser::parse(&parser, bufread);
            QuadIter::from_quad_source(quads)
        }
        NTriples => {
            let parser = NTriplesParser::new();
            let triples = TripleParser::parse(&parser, bufread);
            QuadIter::from_quad_source(triples.to_quads())
        }
        RdfXml => {
            let parser = RdfXmlParser { base: Some(base) };
            let triples = TripleParser::parse(&parser, bufread);
            QuadIter::from_quad_source(triples.to_quads())
        }
        TriG => {
            let parser = TriGParser::new()
                .with_base(Some(base.map_unchecked(Box::from).to_iri_ref().to_base()));
            let quads = QuadParser::parse(&parser, bufread);
            QuadIter::from_quad_source(quads)
        }
        Turtle => {
            let parser = TurtleParser::new()
                .with_base(Some(base.map_unchecked(Box::from).to_iri_ref().to_base()));
            let triples = TripleParser::parse(&parser, bufread);
            QuadIter::from_quad_source(triples.to_quads())
        }
    };
    let quads = match relativizer.take() {
        None => quads,
        Some(rel) => rel.relativize_iter(quads),
    };
    handler.handle_quads(quads)
}

/// Parse JSON-LD or variants (YAML-LD)
fn parse_x_ld<'a, L: sophia::jsonld::loader_factory::LoaderFactory, B: BufRead>(
    format: Format,
    options: JsonLdOptions<L>,
    bufread: B,
) -> Result<QuadIter<'a>> {
    let parser = JsonLdParser::new_with_options(options);
    let quads = if format == YamlLd {
        let value: serde_json::Value = serde_yaml::from_reader(bufread)?;
        let json = serde_json::to_string(&value)?;
        QuadParser::parse(&parser, BufReader::new(json.as_bytes()))
    } else {
        debug_assert_eq!(format, JsonLd);
        QuadParser::parse(&parser, bufread)
    };
    Ok(QuadIter::from_quad_source(quads))
}

fn filename_to_iri(filename: &Path) -> Result<Iri<String>> {
    // TODO make this robust to Windows paths
    let abs = std::path::absolute(filename)?;
    Ok(Iri::new(format!("file://{}", abs.to_string_lossy()))?)
}

fn make_fs_loader(path: Option<&PathBuf>) -> sophia::jsonld::loader::FsLoader {
    let mut ret = sophia::jsonld::loader::FsLoader::default();
    let Some(path) = path else {
        return ret;
    };
    if !path.exists() || !path.is_dir() {
        return ret;
    }

    for res in path
        .read_dir()
        .expect("Can not read entries for `loader_local`")
    {
        match res {
            Err(err) => log::warn!("loader_local entry: {err}"),
            Ok(direntry) => {
                let file_name = direntry.file_name();
                let Some(filename) = file_name.to_str() else {
                    log::debug!(
                        "loader_local: skipping non UTF-8 filename {:?}",
                        direntry.file_name()
                    );
                    continue;
                };
                let entry_path = direntry.path();
                let iri_str: Arc<str> = format!("https://{filename}/").into();
                let Ok(iri) = Iri::new(iri_str) else {
                    log::warn!("loader_local: skipping non-IRI-friendly) {entry_path:?}/");
                    continue;
                };
                log::trace!("loader_local: mounting https://{filename}/ to {entry_path:?}");
                ret.mount(iri, entry_path);
            }
        }
    }
    ret
}

fn make_relativizer(
    base: &Iri<String>,
    rel_arg: Option<Option<u8>>,
) -> Option<Relativizer<String>> {
    rel_arg.map(|opt| {
        let parents = opt.unwrap_or(0);
        let base = BaseIri::new(base.clone().unwrap()).unwrap();
        Relativizer::new(base, parents)
    })
}

fn random_bnode_suffix() -> String {
    let mut bnode_suffix = vec![b'_'; uuid::fmt::Simple::LENGTH + 1];
    uuid::Uuid::new_v4()
        .simple()
        .encode_lower(&mut bnode_suffix[1..]);
    unsafe {
        // SAFETY: uuids only contain ASCII characters
        String::from_utf8_unchecked(bnode_suffix)
    }
}

static ACCEPT: &str = "application/n-quads, application/n-triples, application/trig;q=0.9, text/turtle=q=0.9, application/ld+json;q=0.8, application/rdf+xml;q=0.7, */*;q=0.1";
