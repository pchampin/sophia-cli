use std::io::BufReader;

use anyhow::{Error, Result};
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
    file_or_url::FileOrUrl, format::*, pipe::PipeSubcommand, quad_handler::QuadHandler,
    quad_iter::QuadIter,
};

/// Parse data in an RDF concrete syntax into quads
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// File or URL (- for stdin)
    #[arg(default_value = "-")]
    file_or_url: FileOrUrl,

    /// Format to parse
    #[arg(short, long)]
    format: Option<Format>,

    /// Base IRI against which relative IRIs will be resolve [default: FILE_OR_URL].
    ///
    /// Does not apply to N-Quands, N-Triples or Generalized N-Quads.
    #[arg(short, long, value_parser = |txt: &str| Iri::new(txt.to_string()))]
    base: Option<Iri<String>>,

    /// Process inline contexts only (as opposed to contexts referred by IRI).
    ///
    /// Only applies to JSON-LD.
    #[arg(short, long)]
    inline_contexts_only: bool,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(args: Args) -> Result<()> {
    log::trace!("parse args: {args:#?}");
    let handler = QuadHandler::new(args.pipeline);
    match args.file_or_url {
        FileOrUrl::StdIn => {
            let format = match args.format {
                Some(f) => f,
                None => Err(Error::msg("Cannot guess format for stdin"))?,
            };
            let read = std::io::stdin();
            let base = args
                .base
                .unwrap_or_else(|| Iri::new_unchecked("x-stdin://".into()));
            parse_read(read, format, base, args.inline_contexts_only, handler)
        }
        FileOrUrl::File(filename) => {
            let format = match args.format {
                Some(f) => f,
                None => match filename.rsplit(".").next() {
                    Some(ext) => ext.parse(),
                    None => Err(Error::msg("Cannot guess format for file {filename}")),
                }?,
            };
            let read = std::fs::File::open(&filename)?;
            let base = match args.base {
                Some(b) => b,
                None => filename_to_iri(&filename)?,
            };
            parse_read(read, format, base, args.inline_contexts_only, handler)
        }
        FileOrUrl::Url(url) => {
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
            parse_read(resp, format, base, args.inline_contexts_only, handler)
        }
    }
}

fn parse_read<R: std::io::Read>(
    read: R,
    format: Format,
    base: Iri<String>,
    inline_contexts_only: bool,
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
            if inline_contexts_only {
                let options = JsonLdOptions::new()
                    .with_base(base.map_unchecked(std::sync::Arc::from))
                    .with_document_loader_closure(sophia::jsonld::loader::NoLoader::new);
                let parser = JsonLdParser::new_with_options(options);
                let quads = QuadParser::parse(&parser, bufread);
                handler.handle_quads(QuadIter::from_quad_source(quads))
            } else {
                let options = JsonLdOptions::new()
                    .with_base(base.map_unchecked(std::sync::Arc::from))
                    .with_document_loader_closure(|| {
                        sophia::jsonld::loader::ChainLoader::new(
                            sophia::jsonld::loader::FileUrlLoader::default(),
                            sophia::jsonld::loader::HttpLoader::default(),
                        )
                    });
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

fn filename_to_iri(filename: &str) -> Result<Iri<String>> {
    // TODO make this robust to Windows paths
    let path = std::path::absolute(filename)?;
    Ok(Iri::new(format!("file://{}", path.to_string_lossy()))?)
}

static ACCEPT: &str = "application/n-quads, application/n-triples, application/trig;q=0.9, text/turtle=q=0.9, application/ld+json;q=0.8, application/rdf+xml;q=0.7, */*;q=0.1";
