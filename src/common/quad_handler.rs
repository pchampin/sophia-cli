//! I define the [`QuadHandler`] enum,
//! which provides post-processing of the result of a sub-command returning triples or quads.

use std::io::Write;

use anyhow::Result;
use sophia::{
    api::{
        quad::{Quad, Spog},
        source::QuadSource,
        term::BnodeId,
    },
    term::ArcTerm,
    turtle::serializer::nt::write_term,
};

use super::quad_iter::QuadIter;

pub enum QuadHandler<'a> {
    Stdout,
    Pipeline(crate::SinkSubcommand),
    Sender {
        name: String,
        bnode_suffix: String,
        tx: &'a std::sync::mpsc::Sender<Spog<ArcTerm>>,
    },
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
            QuadHandler::Sender {
                name,
                bnode_suffix,
                tx,
            } => {
                quads
                    .as_iter()
                    .map(|i| i.map_err(|err| log::warn!("{name}: {err}")))
                    .take_while(Result::is_ok) // prevent looping on the same error, which some parsers do
                    .map(Result::unwrap)
                    .map(|quad| add_bnode_suffix_q(quad, &bnode_suffix))
                    .for_each(|i| tx.send(i).map_err(|err| log::warn!("{err}")).unwrap());
                Ok(())
            }
        }
    }
}

fn add_bnode_suffix_q((spo, g): Spog<ArcTerm>, suffix: &str) -> Spog<ArcTerm> {
    (
        spo.map(|t| add_bnode_suffix_t(t, suffix)),
        g.map(|gn| add_bnode_suffix_t(gn, suffix)),
    )
}

fn add_bnode_suffix_t(term: ArcTerm, suffix: &str) -> ArcTerm {
    match term {
        ArcTerm::BlankNode(bnode_id) => ArcTerm::BlankNode(BnodeId::new_unchecked(
            format!("{}{suffix}", bnode_id.as_str()).into(),
        )),
        ArcTerm::Triple(triple) => ArcTerm::Triple(
            <[ArcTerm; 3]>::clone(&triple)
                .map(|t| add_bnode_suffix_t(t, suffix))
                .into(),
        ),
        _ => term,
    }
}
