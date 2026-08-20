//! I define the [`QuadHandler`] enum,
//! which provides post-processing of the result of a sub-command returning triples or quads.

use anyhow::Result;
use sophia::{
    api::{quad::Spog, serializer::QuadSerializer, term::BnodeId},
    term::ArcTerm,
};

use super::quad_iter::QuadIter;

/// Quads are conveyed between threads in batches, to amortize the cost of synchronization.
const QUAD_BATCH_SIZE: usize = 512;

/// How many batches may sit in a [`quad_channel`] before producers are throttled.
///
/// Without such a bound, a producer that is faster than its consumer
/// (e.g. parsing N-Triples, which is much cheaper than `map`'s per-quad SPARQL evaluation)
/// makes the queue grow until memory runs out.
const QUAD_CHANNEL_BOUND: usize = 32;

type QuadBatch = Vec<Spog<ArcTerm>>;

/// Create the bounded channel used to convey quads between threads.
///
/// The bound is what applies back-pressure on producers,
/// keeping memory usage constant regardless of the input size.
pub fn quad_channel() -> (
    std::sync::mpsc::SyncSender<QuadBatch>,
    std::sync::mpsc::Receiver<QuadBatch>,
) {
    std::sync::mpsc::sync_channel(QUAD_CHANNEL_BOUND)
}

/// Accumulate quads and send them over a [`quad_channel`] by batches.
///
/// Any quad left in the current batch is flushed on drop.
pub struct BatchSender<'a> {
    tx: &'a std::sync::mpsc::SyncSender<QuadBatch>,
    batch: QuadBatch,
}

impl<'a> BatchSender<'a> {
    pub fn new(tx: &'a std::sync::mpsc::SyncSender<QuadBatch>) -> Self {
        Self {
            tx,
            batch: Vec::with_capacity(QUAD_BATCH_SIZE),
        }
    }

    pub fn send(&mut self, quad: Spog<ArcTerm>) {
        self.batch.push(quad);
        if self.batch.len() == QUAD_BATCH_SIZE {
            self.flush();
        }
    }

    /// Send the current batch, which must not be empty.
    fn flush(&mut self) {
        debug_assert!(!self.batch.is_empty());
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(QUAD_BATCH_SIZE));
        if let Err(err) = self.tx.send(batch) {
            log::warn!("{err}");
        }
    }
}

impl Drop for BatchSender<'_> {
    fn drop(&mut self) {
        // Unlike `send`, this is the one caller that can have nothing left to send.
        if !self.batch.is_empty() {
            self.flush();
        }
    }
}

pub enum QuadHandler<'a> {
    Stdout,
    Pipeline(crate::SinkSubcommand),
    Sender {
        name: String,
        bnode_suffix: String,
        tx: &'a std::sync::mpsc::SyncSender<QuadBatch>,
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
                sophia::turtle::serializer::nq::NQuadsSerializer::new(std::io::stdout())
                    .serialize_quads(quads)?;
                Ok(())
            }
            QuadHandler::Pipeline(sink) => sink.handle_quads(quads),
            QuadHandler::Sender {
                name,
                bnode_suffix,
                tx,
            } => {
                let mut sender = BatchSender::new(tx);
                quads
                    .as_iter()
                    .map(|i| i.map_err(|err| log::warn!("{name}: {err}")))
                    .take_while(Result::is_ok) // prevent looping on the same error, which some parsers do
                    .map(Result::unwrap)
                    .map(|quad| add_bnode_suffix_q(quad, &bnode_suffix))
                    .for_each(|i| sender.send(i)); // sender flushes its last batch on drop
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
