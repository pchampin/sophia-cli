use std::sync::Arc;

use anyhow::Result;
use sophia::{
    api::{quad::Spog, source::QuadSource},
    iri::{resolve::BaseIri, Iri, IriRef},
    term::ArcTerm,
};

use crate::common::{
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterItem},
};

/// Absolutize all IRIs against the given base.
///
/// See also the `relativize` subcommand.
#[derive(clap::Args, Clone, Debug)]
#[command(verbatim_doc_comment)]
pub struct Args {
    /// Base IRI
    #[arg(value_parser = |txt: &str| Iri::new(txt.to_string()), verbatim_doc_comment)]
    base: Iri<String>,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("absolutize args: {args:#?}");
    let base = BaseIri::new(args.base.as_str()).unwrap();
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(base.absolutize_iter(quads))
}

pub trait BaseIriExt {
    fn absolutize_iter<'s>(self, quads: impl Iterator<Item = QuadIterItem> + 's) -> QuadIter<'s>
    where
        Self: 's;
    fn absolutize_quad(&self, quad: Spog<ArcTerm>) -> Spog<ArcTerm>;
    fn absolutize_term(&self, term: ArcTerm) -> ArcTerm;
}

impl<T> BaseIriExt for BaseIri<T>
where
    T: std::ops::Deref<Target = str>,
{
    fn absolutize_iter<'s>(self, quads: impl Iterator<Item = QuadIterItem> + 's) -> QuadIter<'s>
    where
        Self: 's,
    {
        QuadIter::new(
            quads
                .map_quads(move |q| self.absolutize_quad(q))
                .into_iter(),
        )
    }

    fn absolutize_quad(&self, quad: Spog<ArcTerm>) -> Spog<ArcTerm> {
        let ([s, p, o], g) = quad;
        (
            [
                self.absolutize_term(s),
                self.absolutize_term(p),
                self.absolutize_term(o),
            ],
            g.map(|gn| self.absolutize_term(gn)),
        )
    }

    fn absolutize_term(&self, term: ArcTerm) -> ArcTerm {
        match term {
            ArcTerm::Iri(iri_ref) => {
                ArcTerm::Iri(IriRef::new_unchecked(self.resolve(iri_ref).unwrap().into()))
            }
            ArcTerm::Triple(spo) => ArcTerm::Triple(Arc::new([
                self.absolutize_term(spo[0].clone()),
                self.absolutize_term(spo[1].clone()),
                self.absolutize_term(spo[2].clone()),
            ])),
            _ => term,
        }
    }
}
