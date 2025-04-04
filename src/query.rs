use anyhow::{bail, Context, Result};
use sophia::{
    api::{
        ns::xsd,
        quad::Spog,
        source::{QuadSource, TripleSource},
        sparql::{SparqlDataset, SparqlResult},
        term::Term,
    },
    inmem::{dataset::FastDataset, index::TermIndexFullError},
    sparql::{Bindings, ResultTerm, SparqlWrapper},
    term::ArcTerm,
};

use crate::common::{
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterError},
};

/// Execute a SPARQL query against the quads
///
/// The result can be piped to subcommands if
/// - the query is a CONSTRUCT or a DESCRIBED query, or
/// - the query is a SELECT query with variables ?s, ?p, ?o and (optionally) ?g.
#[derive(clap::Args, Clone, Debug)]
#[command(verbatim_doc_comment)]
pub struct Args {
    /// SPARQL query
    #[arg()]
    query: String,

    /// No not output column headers (variable names) for bindings
    ///
    /// This flag is ignored if query is not SELECT.
    #[arg(short = 'H', long)]
    no_headers: bool,

    /// Exit with an error status if boolean result is `false` (ASK only)
    ///
    /// The result of the query will also not be printed to the output.
    /// This flag is ignored if query is not ASK.
    #[arg(short, long, verbatim_doc_comment)]
    status: bool,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("query args: {args:#?}");
    let dataset: FastDataset = quads.collect_quads()?;
    let sparql = SparqlWrapper(&dataset);
    match sparql.query(&args.query[..]).context("SPARQL error")? {
        SparqlResult::Bindings(bindings) => handle_bindings(bindings, args)?,
        SparqlResult::Boolean(response) => handle_boolean(response, args)?,
        SparqlResult::Triples(triples) => handle_triples(triples, args)?,
    };
    Ok(())
}

fn handle_bindings(bindings: Bindings<FastDataset>, args: Args) -> Result<()> {
    let vars = bindings.variables();
    if let Some(pipeline) = args.pipeline {
        // TODO combine the check and the extraction on indices
        let Some(extractor) = QuadExtractor::try_new(&vars) else {
            bail!("Can not only pipe bindings to sub-command if variables are ?s, ?p, ?o and optionally ?g.")
        };
        let handler = QuadHandler::new(Some(pipeline));
        handler.handle_quads(QuadIter::new(bindings.into_iter().filter_map(
            |res| match res {
                Ok(b) => Ok(extractor.extract(b)).transpose(),
                Err(err) => Some(Err(QuadIterError::new(err))),
            },
        )))
    } else {
        if !args.no_headers {
            println!("?{}", vars.join("\t?"));
        }

        let mut seps = vec!["\t"; vars.len()];
        seps[vars.len() - 1] = "\n";

        for res in bindings {
            for (opt, sep) in res?.into_iter().zip(&seps) {
                if let Some(value) = opt {
                    pretty_print(value);
                }
                print!("{sep}");
            }
        }
        Ok(())
    }
}

fn pretty_print(term: ResultTerm) {
    if let Some(dt) = term.datatype() {
        let lex = term.lexical_form().unwrap();
        if xsd::string == dt {
            print!("{lex:?}");
            return;
        }
        if xsd::boolean == dt || xsd::decimal == dt || xsd::double == dt || xsd::integer == dt {
            print!("{lex}");
            return;
        }
    }
    print!("{term}");
}

fn handle_boolean(response: bool, args: Args) -> Result<()> {
    if args.pipeline.is_some() {
        bail!("Can not pipe boolean result to sub-command")
    } else if args.status {
        std::process::exit(if response { 0 } else { 128 })
    } else {
        println!("{response}");
        Ok(())
    }
}

fn handle_triples(
    triples: Box<dyn Iterator<Item = Result<[ResultTerm; 3], TermIndexFullError>>>,
    args: Args,
) -> Result<()> {
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(QuadIter::from_quad_source(
        triples.map_triples(|spo| (spo, None)),
    ))?;
    Ok(())
}

struct QuadExtractor {
    is: usize,
    ip: usize,
    io: usize,
    ig: Option<usize>,
}

impl QuadExtractor {
    fn try_new(variables: &[&str]) -> Option<Self> {
        if variables.len() < 3 || variables.len() > 4 {
            return None;
        }
        let [mut is, mut ip, mut io, mut ig] = [None; 4];
        for (i, v) in variables.iter().enumerate() {
            match *v {
                "s" => is = Some(i),
                "p" => ip = Some(i),
                "o" => io = Some(i),
                "g" => ig = Some(i),
                _ => return None,
            }
        }
        Some(Self {
            is: is?,
            ip: ip?,
            io: io?,
            ig,
        })
    }

    fn extract(&self, mut b: Vec<Option<ResultTerm>>) -> Option<Spog<ArcTerm>> {
        Some((
            [
                b[self.is].take()?.unwrap(),
                b[self.ip].take()?.unwrap(),
                b[self.io].take()?.unwrap(),
            ],
            self.ig.and_then(|ig| b[ig].take().map(ResultTerm::unwrap)),
        ))
    }
}
