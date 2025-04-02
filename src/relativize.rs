use std::sync::Arc;

use anyhow::Result;
use sophia::{
    api::{quad::Spog, source::QuadSource},
    iri::{is_absolute_iri_ref, relativize::Relativizer, resolve::BaseIri, Iri, IriRef},
    term::ArcTerm,
};

use crate::common::{
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterItem},
};

/// Relativize all IRIs against the given base.
#[derive(clap::Args, Clone, Debug)]
#[command(verbatim_doc_comment)]
pub struct Args {
    /// Base IRI
    #[arg(value_parser = |txt: &str| Iri::new(txt.to_string()), verbatim_doc_comment)]
    base: Iri<String>,

    /// How many parent level to relativize against.
    ///
    /// For example, if 0, no IRI reference starting with '..' will be generated.
    /// If 2, IRI references starting with '..' or '../..' may be generated,
    /// but not with '../../..' .
    #[arg(short, long, default_value = "0", verbatim_doc_comment)]
    parents: u8,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("filter args: {args:#?}");
    let base = BaseIri::new(args.base.as_str()).unwrap();
    let relativizer = Relativizer::new(base, args.parents);
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(relativizer.relativize_iter(quads))
}

pub trait RelativizerExt {
    fn relativize_iter<'s>(
        &'s self,
        quads: impl Iterator<Item = QuadIterItem> + 's,
    ) -> QuadIter<'s>;
    fn relativize_quad(&self, quad: Spog<ArcTerm>) -> Spog<ArcTerm>;
    fn relativize_term(&self, term: ArcTerm) -> ArcTerm;
    fn relativize_iriref(&self, iriref: IriRef<Arc<str>>) -> IriRef<Arc<str>>;
}

impl RelativizerExt for Relativizer<&str> {
    fn relativize_iter<'s>(
        &'s self,
        quads: impl Iterator<Item = QuadIterItem> + 's,
    ) -> QuadIter<'s> {
        QuadIter::new(quads.map_quads(|q| self.relativize_quad(q)).into_iter())
    }

    fn relativize_quad(&self, quad: Spog<ArcTerm>) -> Spog<ArcTerm> {
        let ([s, p, o], g) = quad;
        (
            [
                self.relativize_term(s),
                self.relativize_term(p),
                self.relativize_term(o),
            ],
            g.map(|gn| self.relativize_term(gn)),
        )
    }

    fn relativize_term(&self, term: ArcTerm) -> ArcTerm {
        match term {
            ArcTerm::Iri(iri_ref) => ArcTerm::Iri(self.relativize_iriref(iri_ref)),
            ArcTerm::Triple(spo) => ArcTerm::Triple(Arc::new([
                self.relativize_term(spo[0].clone()),
                self.relativize_term(spo[1].clone()),
                self.relativize_term(spo[2].clone()),
            ])),
            _ => term,
        }
    }

    fn relativize_iriref(&self, iriref: IriRef<Arc<str>>) -> IriRef<Arc<str>> {
        if is_absolute_iri_ref(iriref.as_str()) {
            let iri = Iri::new_unchecked(iriref.as_str());
            self.relativize(iri)
                .map(|i| i.map_unchecked(Into::into))
                .unwrap_or(iriref)
        } else {
            iriref
        }
    }
}
