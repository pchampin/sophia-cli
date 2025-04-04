use anyhow::Result;
use sophia::api::source::QuadSource;

use crate::common::{
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterItem},
};

/// Merge all named graphs into the default graph
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// Drop named graphs and keep only the (merged) default grapĥ
    #[arg(short, long)]
    drop: bool,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("merge-default-graph args: {args:#?}");
    let handler = QuadHandler::new(args.pipeline);
    if args.drop {
        handler.handle_quads(QuadIter::new(
            quads.map_quads(|(spo, _)| (spo, None)).into_iter(),
        ))
    } else {
        handler.handle_quads(QuadIter::new(MergeDefaultGraph::new(quads)))
    }
}

struct MergeDefaultGraph<'a> {
    quads: QuadIter<'a>,
    buffer: Option<QuadIterItem>,
    ended: bool,
}

impl<'a> MergeDefaultGraph<'a> {
    fn new(quads: QuadIter<'a>) -> Self {
        Self {
            quads,
            buffer: None,
            ended: false,
        }
    }
}

impl Iterator for MergeDefaultGraph<'_> {
    type Item = QuadIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.ended, self.buffer.take()) {
            (true, _) => None,
            (false, None) => {
                self.buffer = self.quads.next();
                if self.buffer.is_none() {
                    self.ended = true;
                }
                self.next()
            }
            (false, Some(Ok((spo, Some(g))))) => {
                // a quad with a graph name;
                // duplicate the triple in the default graph (for next time)
                self.buffer = Some(Ok((spo.clone(), None)));
                // then pass the original quad through
                Some(Ok((spo, Some(g))))
            }
            (false, Some(res)) => {
                // either an error or a quad with no graph name;
                // simply pass it through
                Some(res)
            }
        }
    }
}
