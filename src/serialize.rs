use std::{
    io::{stdout, BufWriter, Write},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::{anyhow, Context, Result};
use sophia::{
    api::{
        prefix::PrefixMapPair,
        quad::Quad,
        serializer::{QuadSerializer, TripleSerializer},
        source::{
            QuadSource,
            StreamError::{SinkError, SourceError},
        },
    },
    jsonld::{JsonLdOptions, JsonLdSerializer},
    turtle::serializer::{
        nq::NqSerializer,
        nt::NtSerializer,
        trig::{TrigConfig, TrigSerializer},
        turtle::{TurtleConfig, TurtleSerializer},
    },
    xml::serializer::{RdfXmlConfig, RdfXmlSerializer},
};

use crate::common::{format::Format, prefix_map::parse_prefix_map, quad_iter::QuadIter};

/// Serialize quads to an RDF concrete syntax
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    #[clap(flatten)]
    pub(crate) main: SerializerArgs,

    #[command(flatten)]
    pub(crate) options: SerializerOptions,
}

#[derive(clap::Args, Clone, Debug)]
#[group(required = true, multiple = true)]
pub struct SerializerArgs {
    /// Format to serialize in (required if --output is absent or ambiguous)
    #[arg(short, long)]
    pub(crate) format: Option<Format>,

    /// File to serialize into [default: standard output]
    #[arg(short, long)]
    pub(crate) output: Option<String>,
}

/// Reusable serializer options
#[derive(clap::Args, Clone, Debug)]
pub struct SerializerOptions {
    /// Prefix map expressed as PREFIX:URI,PREFIX:URI,...
    ///
    /// Available for Turtle, TriG.
    #[arg(short, long, value_parser=parse_prefix_map, verbatim_doc_comment)]
    prefixes: Option<PrefixMap>,

    /// Disable pretty-printing
    ///
    /// Available for JSON-LD, RDF/XML, Turtle, TriG.
    #[arg(short = 'P', long, verbatim_doc_comment)]
    no_pretty: bool,
}

type PrefixMap = Vec<PrefixMapPair>;

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("serialize args: {args:#?}");
    match args.main.output.as_ref() {
        None => serialize_to_write(quads, args, stdout()),
        Some(filename) => {
            let file = std::fs::File::create(filename)?;
            serialize_to_write(quads, args, file)
        }
    }
}

pub fn serialize_to_write<W: Write>(quads: QuadIter, mut args: Args, write: W) -> Result<()> {
    let out = std::io::BufWriter::new(write);
    let format = args
        .main
        .format
        .ok_or("")
        .or_else(|_| {
            let filename = args.main.output.as_ref().unwrap(); // output is required if format is absent
            let ext = filename.rsplit(".").next().unwrap();
            ext.parse::<Format>().map_err(|_| filename.as_str())
        })
        .map_err(|filename| {
            anyhow!("Can not guess format for file {filename:?}, please specify with --format")
        })?;
    match format {
        Format::GeneralizedTriG => {
            todo!()
        }
        Format::JsonLd => {
            let indent = if args.options.no_pretty { 0 } else { 2 };
            let options = JsonLdOptions::new().with_spaces(indent);
            let ser = JsonLdSerializer::new_with_options(out, options);
            serialize_quads(quads, ser)
        }
        Format::NQuads | Format::GeneralizedNQuads => {
            let ser = NqSerializer::new(out);
            serialize_quads(quads, ser)
        }
        Format::NTriples => {
            let ser = NtSerializer::new(out);
            serialize_triples(quads, ser)
        }
        Format::RdfXml => {
            let indent = if args.options.no_pretty { 0 } else { 4 };
            let config = RdfXmlConfig::new().with_indentation(indent);
            let ser = RdfXmlSerializer::new_with_config(out, config);
            serialize_triples(quads, ser)
        }
        Format::TriG => {
            let mut config = TrigConfig::new().with_pretty(!args.options.no_pretty);
            if let Some(prefixes) = args.options.prefixes.take() {
                let mut prefix_map = TrigConfig::default_prefix_map();
                prefix_map.extend(prefixes);
                config = config.with_own_prefix_map(prefix_map);
            }
            let ser = TrigSerializer::new_with_config(out, config);
            serialize_quads(quads, ser)
        }
        Format::Turtle => {
            let mut config = TurtleConfig::new().with_pretty(!args.options.no_pretty);
            if let Some(prefixes) = args.options.prefixes.take() {
                let mut prefix_map = TurtleConfig::default_prefix_map();
                prefix_map.extend(prefixes);
                config = config.with_own_prefix_map(prefix_map);
            }
            let ser = TurtleSerializer::new_with_config(out, config);
            serialize_triples(quads, ser)
        }
        Format::YamlLd => {
            let mut json_buf = vec![];
            let ser = JsonLdSerializer::new(BufWriter::new(&mut json_buf));
            serialize_quads(quads, ser)?;
            let val: serde_json::Value = serde_json::from_reader(&json_buf[..])?;
            serde_yaml::to_writer(out, &val).with_context(|| "Error in converting to YAML")
        }
    }
}

fn serialize_triples<S: TripleSerializer>(mut quads: QuadIter, mut ser: S) -> Result<()>
where
    <S as TripleSerializer>::Error: Send + Sync,
{
    let warned = AtomicBool::new(false);
    let triples = quads
        .filter_quads(|q| {
            if q.g().is_some() {
                if !warned.fetch_or(true, Ordering::Relaxed) {
                    log::warn!("Named graphs are ignored when serializing to triples-only format.");
                }
                false
            } else {
                true
            }
        })
        .to_triples();
    match ser.serialize_triples(triples) {
        Ok(_) => Ok(()),
        Err(SourceError(e)) => Err(e).with_context(|| "Error in incoming triples"),
        Err(SinkError(e)) => Err(e).with_context(|| "Error in serializing triples"),
    }
}

fn serialize_quads<S: QuadSerializer>(mut quads: QuadIter, mut ser: S) -> Result<()>
where
    <S as QuadSerializer>::Error: Send + Sync,
{
    match ser.serialize_quads(quads.as_iter()) {
        Ok(_) => Ok(()),
        Err(SourceError(e)) => Err(e).with_context(|| "Error in incoming triples"),
        Err(SinkError(e)) => Err(e).with_context(|| "Error in serializing triples"),
    }
}
