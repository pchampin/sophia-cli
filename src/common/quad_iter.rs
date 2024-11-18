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
pub struct QuadIter<'a>(Box<dyn Iterator<Item = QuadIterItem> + 'a>);

impl<'a> QuadIter<'a> {
    pub fn new<I>(quads: I) -> Self
    where
        I: Iterator<Item = QuadIterItem> + 'a,
    {
        Self(Box::new(quads))
    }

    pub fn from_arcterm_quad_source<Q>(quads: Q) -> Self
    where
        Q: for<'x> QuadSource<Quad<'x> = Spog<ArcTerm>> + 'a,
        anyhow::Error: From<<Q as QuadSource>::Error>,
    {
        Self::new(
            quads
                .map_quads(|q| q)
                .into_iter()
                .map(|res| res.map_err(QuadIterError::new)),
        )
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

    /// Expose the inner [`Iterator`] (and [`QuadSource`]) of this [`QuadIter`]
    pub fn as_iter(&mut self) -> &mut dyn Iterator<Item = QuadIterItem> {
        &mut self.0
    }
}

impl<'a> std::ops::Deref for QuadIter<'a> {
    type Target = dyn Iterator<Item = QuadIterItem> + 'a;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> std::ops::DerefMut for QuadIter<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

//

/// The type of items that [`QuadIter`] yields.
pub type QuadIterItem = Result<Spog<ArcTerm>, QuadIterError>;

/// The type of errors that [`QuadIter`] yields.
/// This is actually just a wrapper around [`anyhow::Error`] that makes it implement [`std::error::Error`].
#[derive(Debug)]
pub struct QuadIterError(anyhow::Error);

//

impl QuadIterError {
    pub fn new<E: Into<anyhow::Error>>(err: E) -> Self {
        Self(err.into())
    }
}

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
