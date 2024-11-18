use anyhow::Result;
use sophia::{
    api::{
        quad::{Quad, Spog},
        source::QuadSource,
        term::FromTerm,
    },
    term::ArcTerm,
};

/// The type use to convey quads from one subcommand to the next one.
pub struct QuadIter<'a>(Box<dyn Iterator<Item = Result<Spog<ArcTerm>>> + 'a>);

impl<'a> Iterator for QuadIter<'a> {
    type Item = Result<Spog<ArcTerm>, QuadIterError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|res| res.map_err(QuadIterError))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> QuadIter<'a> {
    pub fn new<I, E>(quads: I) -> Self
    where
        I: Iterator<Item = Result<Spog<ArcTerm>, E>> + 'a,
        anyhow::Error: From<E>,
    {
        Self(Box::new(quads.map(|res| res.map_err(anyhow::Error::from))))
    }

    pub fn from_arcterm_quad_source<Q>(quads: Q) -> Self
    where
        Q: for<'x> QuadSource<Quad<'x> = Spog<ArcTerm>> + 'a,
        anyhow::Error: From<<Q as QuadSource>::Error>,
    {
        Self::new(quads.map_quads(|q| q).into_iter())
    }

    /// Convert an arbitrary [`QuadSource`] into a [`QuadIter`].
    ///
    /// Only use if the terms in the QuadSource are *not* [`ArcTerm`]s,
    /// otherwise, use [`QuadSource::from_arcterm_quad_source`] or [`QuadSource::new`].
    pub fn from_quad_source<Q>(quads: Q) -> Self
    where
        Q: QuadSource + 'a,
        anyhow::Error: From<<Q as QuadSource>::Error>,
    {
        Self::from_arcterm_quad_source(quads.map_quads(|q| {
            let (spo, g) = q.to_spog();
            let spo = spo.map(ArcTerm::from_term);
            let g = g.map(ArcTerm::from_term);
            (spo, g)
        }))
    }
}

#[derive(Debug)]
pub struct QuadIterError(anyhow::Error);

impl std::fmt::Display for QuadIterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for QuadIterError {}

impl From<anyhow::Error> for QuadIterError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}
