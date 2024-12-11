use std::{
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Error, Result};
use rayon::prelude::*;
use sophia::{
    api::{
        parser::{QuadParser, TripleParser},
        source::TripleSource,
    },
    iri::Iri,
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
    quad_iter::QuadIter,
};

/// Parse data in an RDF concrete syntax into quads
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// File or URL (- for stdin)
    ///
    /// To parse multiple files or URLs, use `--multiple` instead.
    /// Otherwise, defaults to stdin.
    #[arg(verbatim_doc_comment)]
    file_or_url: Option<FileOrUrl>,

    /// Multiple filenames, glob patterns or URLs, terminated with 'm-'
    #[arg(short, long, num_args = 1.. , value_terminator = "m-", conflicts_with = "file_or_url")]
    multiple: Vec<FilesOrUrl>,

    /// Format to parse
    #[arg(short, long)]
    format: Option<Format>,

    /// Base IRI against which relative IRIs will be resolve [default: FILE_OR_URL].
    ///
    /// Does not apply to N-Quands, N-Triples or Generalized N-Quads.
    #[arg(short, long, value_parser = |txt: &str| Iri::new(txt.to_string()))]
    base: Option<Iri<String>>,

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
    /// Only applies to JSON-LD.
    ///
    /// Every subdirectory SUBDIR of the given path is interpreted as a local cache
    /// for the https://SUBDIR/ namespace.
    #[arg(short = 'l', long, env = "DOCUMENT_LOADER_CACHE", verbatim_doc_comment)]
    loader_local: Option<PathBuf>,

    /// Fetch unknown contexts from URL.
    ///
    /// Only applies to JSON-LD.
    ///
    /// This is not the default behavior, because fetching unknown contexts from the
    /// web (or the filesystem) is usually not fit for production.
    /// Consider using `--loader-local` instead.
    #[arg(short = 'u', long, verbatim_doc_comment)]
    loader_urls: bool,
}

pub fn run(mut args: Args) -> Result<()> {
    log::trace!("parse args: {args:#?}");
    let handler = QuadHandler::new(args.pipeline.take());
    if args.multiple.is_empty() {
        match args.file_or_url.take().unwrap_or(FileOrUrl::StdIn) {
            FileOrUrl::StdIn => parse_stdin(args, handler),
            FileOrUrl::File(filename) => parse_file(args, &PathBuf::from(filename), handler),
            FileOrUrl::Url(url) => parse_url(args, url, handler),
        }
    } else {
        let (tx, rx) = std::sync::mpsc::channel();
        let sink_thread =
            std::thread::spawn(|| handler.handle_quads(QuadIter::new(rx.into_iter())));
        std::mem::take(&mut args.multiple)
            .into_iter()
            .flat_map(FilesOrUrl::into_iter)
            .par_bridge()
            .for_each(|path_or_url| {
                log::debug!("{path_or_url:?}");
                let handler = QuadHandler::Sender(&tx);
                if let Err(err) = match path_or_url {
                    PathOrUrl::Path(path_buf) => parse_file(args.clone(), &path_buf, handler),
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
    parse_read(read, format, base, args.options, handler)
}

fn parse_file(args: Args, filename: &Path, handler: QuadHandler) -> std::result::Result<(), Error> {
    let format = match args.format {
        Some(f) => f,
        None => match filename.to_string_lossy().rsplit(".").next() {
            Some(ext) => ext.parse(),
            None => Err(Error::msg("Cannot guess format for file {filename}")),
        }?,
    };
    let read = std::fs::File::open(filename)?;
    let base = match args.base {
        Some(b) => b,
        None => filename_to_iri(filename)?,
    };
    parse_read(read, format, base, args.options, handler)
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
    parse_read(resp, format, base, args.options, handler)
}

fn parse_read<R: std::io::Read>(
    read: R,
    format: Format,
    base: Iri<String>,
    options: ParserOptions,
    handler: QuadHandler,
) -> Result<()> {
    let bufread = BufReader::new(read);
    match format {
        GeneralizedNQuads => {
            let parser = GNQuadsParser {};
            let quads = QuadParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(quads))
        }
        GeneralizedTriG => {
            let parser = GTriGParser { base: Some(base) };
            let quads = QuadParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(quads))
        }
        JsonLd => {
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
                let parser = JsonLdParser::new_with_options(options);
                let quads = QuadParser::parse(&parser, bufread);
                handler.handle_quads(QuadIter::from_quad_source(quads))
            } else {
                let options = JsonLdOptions::new()
                    .with_base(base.map_unchecked(std::sync::Arc::from))
                    .with_document_loader_closure(|| make_fs_loader(options.loader_local.as_ref()));
                let parser = JsonLdParser::new_with_options(options);
                let quads = QuadParser::parse(&parser, bufread);
                handler.handle_quads(QuadIter::from_quad_source(quads))
            }
        }
        NQuads => {
            let parser = NQuadsParser {};
            let quads = QuadParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(quads))
        }
        NTriples => {
            let parser = NTriplesParser {};
            let triples = TripleParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(triples.to_quads()))
        }
        RdfXml => {
            let parser = RdfXmlParser { base: Some(base) };
            let triples = TripleParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(triples.to_quads()))
        }
        TriG => {
            let parser = TriGParser { base: Some(base) };
            let quads = QuadParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(quads))
        }
        Turtle => {
            let parser = TurtleParser { base: Some(base) };
            let triples = TripleParser::parse(&parser, bufread);
            handler.handle_quads(QuadIter::from_quad_source(triples.to_quads()))
        }
    }
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

static ACCEPT: &str = "application/n-quads, application/n-triples, application/trig;q=0.9, text/turtle=q=0.9, application/ld+json;q=0.8, application/rdf+xml;q=0.7, */*;q=0.1";
