//! I define the [`QuadHandler`] enum,
//! which provides post-processing of the result of a sub-command returning triples or quads.

use std::io::Write;

use anyhow::Result;
use sophia::{
    api::{
        quad::{Quad, Spog},
        source::{QuadSource, TripleSource},
        triple::Triple,
    },
    turtle::serializer::nt::write_term,
};

pub enum QuadHandler {
    Stdout,
    Pipeline(crate::SinkSubcommand),
}

impl QuadHandler {
    pub fn new(pipeline: Option<crate::common::pipe::PipeSubcommand>) -> Self {
        match pipeline {
            None => Self::Stdout,
            Some(pipe) => Self::Pipeline(pipe.parse()),
        }
    }

    pub fn handle_quads<Q: QuadSource>(self, mut quads: Q) -> Result<()>
    where
        <Q as QuadSource>::Error: Send + Sync,
    {
        match self {
            QuadHandler::Stdout => {
                let mut stdout = std::io::stdout();
                quads.try_for_each_quad(|q| {
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
        }
    }

    pub fn handle_triples<T: TripleSource>(self, triples: T) -> Result<()>
    where
        <T as TripleSource>::Error: Send + Sync,
    {
        self.handle_quads(TripleToQuad(triples))
    }
}

struct TripleToQuad<T>(T);

impl<T: TripleSource> QuadSource for TripleToQuad<T>
where
    <T as TripleSource>::Error: Send + Sync,
{
    type Quad<'x> = Spog<<<T as TripleSource>::Triple<'x> as Triple>::Term>;

    type Error = <T as TripleSource>::Error;

    fn try_for_some_quad<E, F>(
        &mut self,
        mut f: F,
    ) -> sophia::api::source::StreamResult<bool, Self::Error, E>
    where
        E: std::error::Error,
        F: FnMut(Self::Quad<'_>) -> std::result::Result<(), E>,
    {
        self.0.try_for_some_triple(|t| f(t.into_quad()))
    }
}
