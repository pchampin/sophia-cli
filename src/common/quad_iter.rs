use anyhow::Result;
use sophia::{
    api::{
        quad::{Quad, Spog},
        source::{QuadSource, Source},
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
        Q: for<'x> QuadSource<Item<'x> = Spog<ArcTerm>> + 'a,
        anyhow::Error: From<<Q as Source>::Error>,
    {
        Self::new(
            quads
                .map_quads(|q| q) // to ensure that .into_iter() is available
                .into_iter()
                .map(|res| res.map_err(QuadIterError::new)),
        )
    }

    /// Convert an arbitrary [`QuadSource`] into a [`QuadIter`].
    ///
    /// Only use if the terms in the QuadSource are *not* [`ArcTerm`]s,
    /// otherwise, use [`QuadIter::from_arcterm_quad_source`] or [`QuadIter::new`].
    pub fn from_quad_source<Q>(quads: Q) -> Self
    where
        Q: QuadSource + 'a,
        anyhow::Error: From<<Q as Source>::Error>,
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

impl Iterator for QuadIter<'_> {
    type Item = QuadIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.0.count()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.0.last()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth(n)
    }

    fn for_each<F>(self, f: F)
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        self.0.for_each(f)
    }

    fn partition<B, F>(self, f: F) -> (B, B)
    where
        Self: Sized,
        B: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool,
    {
        self.0.partition(f)
    }

    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.0.fold(init, f)
    }

    fn reduce<F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(Self::Item, Self::Item) -> Self::Item,
    {
        self.0.reduce(f)
    }

    fn all<F>(&mut self, f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        self.0.all(f)
    }

    fn any<F>(&mut self, f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        self.0.any(f)
    }

    fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        self.0.find(predicate)
    }

    fn find_map<B, F>(&mut self, f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        self.0.find_map(f)
    }

    fn position<P>(&mut self, predicate: P) -> Option<usize>
    where
        Self: Sized,
        P: FnMut(Self::Item) -> bool,
    {
        self.0.position(predicate)
    }

    fn max_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        self.0.max_by_key(f)
    }

    fn max_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering,
    {
        self.0.max_by(compare)
    }

    fn min_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        self.0.min_by_key(f)
    }

    fn min_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> std::cmp::Ordering,
    {
        self.0.min_by(compare)
    }

    fn eq<I>(self, other: I) -> bool
    where
        I: IntoIterator,
        Self::Item: PartialEq<I::Item>,
        Self: Sized,
    {
        self.0.eq(other)
    }

    fn ne<I>(self, other: I) -> bool
    where
        I: IntoIterator,
        Self::Item: PartialEq<I::Item>,
        Self: Sized,
    {
        self.0.ne(other)
    }

    fn is_sorted_by<F>(self, compare: F) -> bool
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        self.0.is_sorted_by(compare)
    }

    fn is_sorted_by_key<F, K>(self, f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> K,
        K: PartialOrd,
    {
        self.0.is_sorted_by_key(f)
    }
}

//

/// The type of items that [`QuadIter`] yields.
pub type QuadIterItem = Result<Spog<ArcTerm>, QuadIterError>;

/// Build a [`QuadIterItem`] from any (quad, error) result.
pub fn quad_iter_item<T: Quad, E: Into<anyhow::Error>>(res: Result<T, E>) -> QuadIterItem {
    res.map(|q| -> Spog<ArcTerm> {
        let (spo, g) = q.to_spog();
        let spo = spo.map(ArcTerm::from_term);
        let g = g.map(ArcTerm::from_term);
        (spo, g)
    })
    .map_err(|e| QuadIterError::new(e))
}

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

impl From<std::convert::Infallible> for QuadIterError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}
