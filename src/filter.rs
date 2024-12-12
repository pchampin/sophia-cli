use std::convert::Infallible;

use anyhow::Result;
use sophia::{
    api::{quad::Spog, sparql::SparqlDataset},
    sparql::{SparqlQuery, SparqlWrapper, SparqlWrapperError},
    term::ArcTerm,
};

use crate::common::{pipe::PipeSubcommand, quad_handler::QuadHandler, quad_iter::QuadIter};

/// Keep only quads that match a SPARQL expression
///
/// In the expression, ?s, ?p, ?o and ?g are bound to the subject, predicate,
/// object and graph name of the quad, respectively.
#[derive(clap::Args, Clone, Debug)]
#[command(verbatim_doc_comment)]
pub struct Args {
    /// SPARQL expression
    #[arg()]
    expression: String,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(mut quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("filter args: {args:#?}");

    let ask_query = make_query(&args.expression)?;
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(QuadIter::new(quads.into_iter().filter_map(|res| {
        let Ok(quad) = res else {
            return Some(res); // always keep errors
        };
        let dataset = [quad];
        let sparql = SparqlWrapper(&dataset[..]);
        let resp = sparql.query(&ask_query).ok()?.into_boolean();
        let [quad] = dataset;
        resp.then(|| Ok(quad))
    })))
}

fn make_query(
    expression: &str,
) -> Result<SparqlQuery<[Spog<ArcTerm>]>, SparqlWrapperError<Infallible>> {
    let empty_dataset: [Spog<ArcTerm>; 0] = [];
    let sparql = SparqlWrapper(&empty_dataset[..]);
    sparql.prepare_query(&format!(
        "ASK {{ {{ ?s ?p ?o }} UNION {{ GRAPH ?g {{ ?s ?p ?o }} }} FILTER ({expression}) }}"
    ))
}
