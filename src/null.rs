use anyhow::Result;

use crate::common::quad_iter::QuadIter;

/// Silently consume all quads, and only report errors
///
/// Think of this as `> /dev/null`
#[derive(clap::Args, Clone, Debug)]
pub struct Args {}

pub fn run(quads: QuadIter, _args: Args) -> Result<()> {
    for q in quads.into_iter() {
        q?;
    }
    Ok(())
}
