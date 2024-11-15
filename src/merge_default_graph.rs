use anyhow::Result;
use sophia::api::{quad::Quad, source::QuadSource};

use crate::common::{pipe::PipeSubcommand, quad_handler::QuadHandler};

/// Merge named graphs with the default graph
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run<Q: QuadSource>(quads: Q, args: Args) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
{
    log::trace!("merge-default-graph args: {args:#?}");
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_triples(quads.to_triples())
}
