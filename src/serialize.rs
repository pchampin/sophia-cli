use std::{
    io::{stdout, Write},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use sophia::{
    api::{
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

use crate::common::format::Format;

/// Serialize quads to an RDF concrete syntax
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// Format to serialize
    #[arg()]
    format: Format,

    /// File to serialize into [default: standard output]
    #[arg(short, long)]
    output: Option<String>,

    /// Disable pretty-printing (available for RDF/XML, Turtle, TriG)
    #[arg(short = 'P', long)]
    no_pretty: bool,
}

pub fn run<Q: QuadSource>(quads: Q, mut args: Args) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
{
    log::trace!("serialize args: {args:#?}");
    match args.output.take() {
        None => serialize_to_write(quads, args, stdout()),
        Some(filename) => serialize_to_write(quads, args, std::fs::File::create(filename)?),
    }
}

pub fn serialize_to_write<Q: QuadSource, W: Write>(quads: Q, args: Args, write: W) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
{
    let out = std::io::BufWriter::new(write);
    match args.format {
        Format::GeneralizedTriG => {
            todo!()
        }
        Format::JsonLd => {
            let options = JsonLdOptions::new().with_spaces(2);
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
            /* // available only on Sophia's github for the moment
            let indent = if args.no_pretty { 0 } else { 4 };
            let config = RdfXmlConfig::new().with_indentation(indent);
            */
            let config = RdfXmlConfig {};
            let ser = RdfXmlSerializer::new_with_config(out, config);
            serialize_triples(quads, ser)
        }
        Format::TriG => {
            let config = TrigConfig::new().with_pretty(!args.no_pretty);
            let ser = TrigSerializer::new_with_config(out, config);
            serialize_quads(quads, ser)
        }
        Format::Turtle => {
            let config = TurtleConfig::new().with_pretty(!args.no_pretty);
            let ser = TurtleSerializer::new_with_config(out, config);
            serialize_triples(quads, ser)
        }
    }
}

fn serialize_triples<Q: QuadSource, S: TripleSerializer>(quads: Q, mut ser: S) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
    <S as TripleSerializer>::Error: Send + Sync,
{
    let warned = AtomicBool::new(false);
    let triples = quads.to_triples();
    match ser.serialize_triples(triples) {
        Ok(_) => Ok(()),
        Err(SourceError(e)) => Err(e).with_context(|| "Error in incoming triples"),
        Err(SinkError(e)) => Err(e).with_context(|| "Error in serializing triples"),
    }
}

fn serialize_quads<Q: QuadSource, S: QuadSerializer>(quads: Q, mut ser: S) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
    <S as QuadSerializer>::Error: Send + Sync,
{
    match ser.serialize_quads(quads) {
        Ok(_) => Ok(()),
        Err(SourceError(e)) => Err(e).with_context(|| "Error in incoming triples"),
        Err(SinkError(e)) => Err(e).with_context(|| "Error in serializing triples"),
    }
}
