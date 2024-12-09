//! I define the [`QuadHandler`] enum,
//! which provides post-processing of the result of a sub-command returning triples or quads.

use std::io::Write;

use anyhow::Result;
use sophia::{
    api::{quad::Quad, source::QuadSource},
    turtle::serializer::nt::write_term,
};

use super::quad_iter::{QuadIter, QuadIterItem};

pub enum QuadHandler<'a> {
    Stdout,
    Pipeline(crate::SinkSubcommand),
    Sender(&'a std::sync::mpsc::Sender<QuadIterItem>),
}

impl QuadHandler<'_> {
    pub fn new(pipeline: Option<crate::common::pipe::PipeSubcommand>) -> Self {
        match pipeline {
            None => Self::Stdout,
            Some(pipe) => Self::Pipeline(pipe.parse()),
        }
    }

    pub fn handle_quads(self, mut quads: QuadIter) -> Result<()> {
        match self {
            QuadHandler::Stdout => {
                let mut stdout = std::io::stdout();
                quads.as_iter().try_for_each_quad(|q| {
                    write_term(&mut stdout, q.s())?;
                    stdout.write_all(b"\t")?;
                    write_term(&mut stdout, q.p())?;
                    stdout.write_all(b"\t")?;
                    write_term(&mut stdout, q.o())?;
                    stdout.write_all(b"\t")?;
                    if let Some(g) = q.g() {
                        write_term(&mut stdout, g)?;
                    }
                    stdout.write_all(b"\t.\n")?;
                    Ok(()) as std::io::Result<()>
                })?;
                Ok(())
            }
            QuadHandler::Pipeline(sink) => sink.handle_quads(quads),
            QuadHandler::Sender(tx) => {
                quads
                    .as_iter()
                    .for_each(|i| tx.send(i).map_err(|err| log::warn!("{err}")).unwrap());
                Ok(())
            }
        }
    }
}
