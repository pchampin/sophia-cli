use anyhow::{Context, Result};
use sophia::{
    api::{quad::Spog, source::QuadSource, sparql::SparqlDataset},
    sparql::{SparqlQuery, SparqlWrapper},
    term::ArcTerm,
};

use crate::common::{pipe::PipeSubcommand, quad_handler::QuadHandler, quad_iter::QuadIter};

/// Transform each quad based on SPARQL expressions
///
/// In the expression, ?s, ?p, ?o and ?g are bound to the subject, predicate,
/// object, and graph name of the quad, respectively.
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// SPARQL expression to map subjects to (context: ?s, ?p, ?o, ?g)
    #[arg(short, long, default_value = "?s", value_name = "EXPRESSION")]
    subject: String,

    /// SPARQL expression to map predicates to (context: ?s, ?p, ?o, ?g)
    #[arg(short, long, default_value = "?p", value_name = "EXPRESSION")]
    predicate: String,

    /// SPARQL expression to map objects to (context: ?s, ?p, ?o, ?g)
    #[arg(short, long, default_value = "?o", value_name = "EXPRESSION")]
    object: String,

    /// SPARQL expression to map graph names to (context: ?s, ?p, ?o, ?g)
    #[arg(short, long, default_value = "?g", value_name = "EXPRESSION")]
    graph: String,

    // TODO add an option to only produce strict RDF triples
    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("filter args: {args:#?}");

    let ask_query = make_query(&args)?;
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(QuadIter::new(
        quads
            .filter_map_quads(|quad| {
                let dataset = [quad];
                let sparql = SparqlWrapper(&dataset[..]);
                let resp = sparql.query(&ask_query).ok()?.into_bindings();
                let mut v = resp.into_iter().next()?.ok()?;
                let g = v.pop()?.map(|t| t.unwrap());
                let o = v.pop()??.unwrap();
                let p = v.pop()??.unwrap();
                let s = v.pop()??.unwrap();
                Some(([s, p, o], g))
            })
            .into_iter(),
    ))
}

fn make_query(args: &Args) -> Result<SparqlQuery<[Spog<ArcTerm>]>> {
    let Args {
        subject,
        predicate,
        object,
        graph,
        ..
    } = args;
    let empty_dataset: [Spog<ArcTerm>; 0] = [];
    let sparql = SparqlWrapper(&empty_dataset[..]);
    let q = &format!(
        r#"
        SELECT
        (({subject}) as ?s2)
        (({predicate}) as ?p2)
        (({object}) as ?o2)
        (({graph}) as ?g2)
        WHERE
        {{ {{ ?s ?p ?o }} UNION {{ GRAPH ?g {{ ?s ?p ?o }} }} }}
    "#
    );
    sparql
        .prepare_query(q)
        .with_context(|| format!("Parsing map SPARQL query:{q}"))
}
