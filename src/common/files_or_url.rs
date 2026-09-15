use std::{
    fmt::Display,
    mem::take,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::{Error, Result};
use globset::{GlobBuilder, GlobMatcher};
use regex::Regex;
use reqwest::Url;
use walkdir::WalkDir;

#[derive(Clone, Debug)]
pub enum FilesOrUrl {
    File(String),
    Glob(GlobPattern),
    Url(Url),
}

impl std::str::FromStr for FilesOrUrl {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        static URL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new("^http(s)?://").unwrap());
        if URL_RE.is_match(value) {
            Ok(FilesOrUrl::Url(Url::parse(value)?))
        } else if std::fs::exists(value)? {
            Ok(FilesOrUrl::File(value.to_string()))
        } else if let Ok(pattern) = GlobPattern::new(value) {
            Ok(FilesOrUrl::Glob(pattern))
        } else {
            Err(Error::msg(format!(
                "Neither an http(s) URL, an existing file or a valid glog pattern: {value}"
            )))
        }
    }
}

impl std::fmt::Display for FilesOrUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            FilesOrUrl::File(s) => s,
            FilesOrUrl::Glob(s) => s.as_str(),
            FilesOrUrl::Url(s) => s.as_str(),
        };
        txt.fmt(f)
    }
}

impl IntoIterator for FilesOrUrl {
    type Item = PathOrUrl;

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            FilesOrUrl::File(filename) => IntoIter::File(filename.into()),
            FilesOrUrl::Glob(pattern) => pattern.into_iter(),
            FilesOrUrl::Url(url) => IntoIter::Url(url),
        }
    }
}

/// A glob pattern, compiled for matching and prepared for walking the filesystem.
///
/// [`globset`] only *matches* paths, it does not expand a pattern into the files it matches,
/// so we walk the filesystem ourselves and match every path we encounter.
/// To keep that walk cheap, it starts at [`root`](Self::root) — the longest leading part of the
/// pattern that contains no metacharacter — and stops at [`max_depth`](Self::max_depth).
#[derive(Clone, Debug)]
pub struct GlobPattern {
    /// The pattern as written by the user, kept for display and warnings.
    pattern: String,
    matcher: GlobMatcher,
    /// The directory (or file) to start walking from.
    root: PathBuf,
    /// Whether `root` was inferred rather than written in the pattern.
    ///
    /// When it was, `root` is `.` and the walked paths must be stripped of their leading `./`,
    /// so that they match the pattern (and are reported) the way the user wrote them.
    implicit_root: bool,
    /// How deep to walk below `root`, or `None` when the pattern contains `**`.
    max_depth: Option<usize>,
}

impl GlobPattern {
    pub fn new(pattern: &str) -> Result<Self, globset::Error> {
        let matcher = GlobBuilder::new(pattern)
            // so that '*' does not match across directories, and '**' is the recursive wildcard
            .literal_separator(true)
            .build()?
            .compile_matcher();
        let (root, implicit_root, max_depth) = plan_walk(pattern);
        Ok(Self {
            pattern: pattern.to_string(),
            matcher,
            root,
            implicit_root,
            max_depth,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.pattern
    }
}

impl IntoIterator for GlobPattern {
    type Item = PathOrUrl;

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let Self {
            pattern,
            matcher,
            root,
            implicit_root,
            max_depth,
        } = self;
        // Walking a root that does not exist would report an error for every pattern that
        // happens to match nothing; the 'matches no file' warning below is more helpful.
        if !root.exists() {
            log::warn!("Pattern '{pattern}' matches no file");
            return IntoIter::End;
        }
        // `max_depth == Some(0)` means the pattern is entirely literal, so `root` is the
        // candidate itself; otherwise the candidates are strictly below `root`.
        let min_depth = usize::from(max_depth != Some(0));
        let mut walk = WalkDir::new(&root)
            .min_depth(min_depth)
            .follow_links(true) // the `glob` crate used to descend into symlinked directories
            .sort_by_file_name(); // ... and to yield paths in a deterministic order
        if let Some(max_depth) = max_depth {
            walk = walk.max_depth(max_depth);
        }
        IntoIter::Glob(Box::new(GlobWalk {
            pattern,
            matcher,
            implicit_root,
            walk: walk.into_iter(),
            found_any: false,
        }))
    }
}

/// Work out where to start walking the filesystem for `pattern`, and how deep to go.
///
/// Returns the root to walk from, whether that root was inferred (rather than written in the
/// pattern), and the maximum depth below it — `None` meaning unlimited, for `**` patterns.
fn plan_walk(pattern: &str) -> (PathBuf, bool, Option<usize>) {
    let absolute = pattern.starts_with('/');
    let components: Vec<&str> = pattern.split('/').filter(|c| !c.is_empty()).collect();
    // Over-detecting a metacharacter only makes the walk start higher, which stays correct,
    // so an escaped metacharacter needs no special case here.
    let literal = components
        .iter()
        .take_while(|c| !c.contains(['*', '?', '[', '{']))
        .count();

    let mut root = PathBuf::new();
    if absolute {
        root.push("/");
    }
    for component in &components[..literal] {
        root.push(component);
    }
    let implicit_root = root.as_os_str().is_empty();
    if implicit_root {
        root.push(".");
    }

    let rest = &components[literal..];
    let max_depth = if rest.iter().any(|c| c.contains("**")) {
        None
    } else {
        Some(rest.len())
    };
    (root, implicit_root, max_depth)
}

#[derive(Default)]
pub enum IntoIter {
    #[default]
    End,
    File(PathBuf),
    /// Boxed, because walking the filesystem carries much more state than the other variants.
    Glob(Box<GlobWalk>),
    Url(Url),
}

impl Iterator for IntoIter {
    type Item = PathOrUrl;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::End => None,
            Self::File(_) => {
                let Self::File(path) = take(self) else {
                    unreachable!()
                };
                Some(PathOrUrl::Path(path))
            }
            Self::Glob(walk) => walk.next(),
            Self::Url(_) => {
                let Self::Url(url) = take(self) else {
                    unreachable!()
                };
                Some(PathOrUrl::Url(url))
            }
        }
    }
}

/// The state of an in-progress walk of the filesystem, matching paths against a pattern.
pub struct GlobWalk {
    pattern: String,
    matcher: GlobMatcher,
    implicit_root: bool,
    walk: walkdir::IntoIter,
    found_any: bool,
}

impl GlobWalk {
    fn next(&mut self) -> Option<PathOrUrl> {
        loop {
            let Some(entry) = self.walk.next() else {
                if !self.found_any {
                    log::warn!("Pattern '{}' matches no file", self.pattern);
                    self.found_any = true; // so that we only warn once
                }
                return None;
            };
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    log::warn!("{err}");
                    continue;
                }
            };
            // Directories can not be parsed, and would only produce spurious errors.
            if entry.file_type().is_dir() {
                continue;
            }
            let path = strip_implicit_root(entry.into_path(), self.implicit_root);
            if self.matcher.is_match(&path) {
                self.found_any = true;
                return Some(PathOrUrl::Path(path));
            }
        }
    }
}

/// Remove the leading `./` that walking an inferred root adds to every path.
fn strip_implicit_root(path: PathBuf, implicit_root: bool) -> PathBuf {
    if !implicit_root {
        return path;
    }
    match path.strip_prefix(Path::new(".")) {
        Ok(stripped) => stripped.to_path_buf(),
        Err(_) => path,
    }
}

#[derive(Clone, Debug)]
pub enum PathOrUrl {
    Path(PathBuf),
    Url(Url),
}

impl Display for PathOrUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathOrUrl::Path(path_buf) => path_buf.to_string_lossy().fmt(f),
            PathOrUrl::Url(url) => url.fmt(f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Expand `pattern` into the paths it matches, relative to the crate root.
    fn expand(pattern: &str) -> Vec<String> {
        let mut paths: Vec<String> = GlobPattern::new(pattern)
            .unwrap()
            .into_iter()
            .map(|p| p.to_string())
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn simple_wildcard() {
        assert_eq!(
            expand("examples/*.nt"),
            [
                "examples/msg-1.nt",
                "examples/msg-2.nt",
                "examples/sample.nt"
            ]
        );
    }

    /// The point of this module: `globset` supports alternates, which `glob` did not.
    #[test]
    fn alternates() {
        assert_eq!(
            expand("examples/*.{nt,ttl}"),
            [
                "examples/lang.ttl",
                "examples/msg-1.nt",
                "examples/msg-2.nt",
                "examples/sample.nt",
                "examples/social.ttl",
            ]
        );
    }

    #[test]
    fn alternates_on_the_stem() {
        assert_eq!(
            expand("examples/{lang,social}.ttl"),
            ["examples/lang.ttl", "examples/social.ttl"]
        );
    }

    #[test]
    fn single_character_wildcard() {
        assert_eq!(
            expand("examples/msg-?.nt"),
            ["examples/msg-1.nt", "examples/msg-2.nt"]
        );
    }

    #[test]
    fn character_range() {
        assert_eq!(
            expand("examples/[ls]*.ttl"),
            ["examples/lang.ttl", "examples/social.ttl"]
        );
    }

    #[test]
    fn negated_character_range() {
        assert_eq!(expand("examples/[!ls]*.ttl"), [] as [&str; 0]);
    }

    #[test]
    fn recursive_wildcard() {
        assert_eq!(expand("examples/**/*.rq"), ["examples/persons.rq"]);
    }

    /// `*` must not cross directory separators, otherwise it would match `examples/sample.nt`.
    #[test]
    fn wildcard_does_not_cross_directories() {
        assert_eq!(expand("*.nt"), [] as [&str; 0]);
    }

    #[test]
    fn no_match() {
        assert_eq!(expand("examples/*.does-not-exist"), [] as [&str; 0]);
    }

    /// A pattern whose root does not exist must not report a walk error.
    #[test]
    fn missing_root() {
        assert_eq!(expand("no-such-directory/*.nt"), [] as [&str; 0]);
    }

    #[test]
    fn directories_are_skipped() {
        let paths = expand("examples/*");
        assert!(paths.contains(&"examples/sample.nt".to_string()));
        assert!(paths.iter().all(|p| !Path::new(p).is_dir()));
    }

    #[test]
    fn plan_walk_infers_root_and_depth() {
        for (pattern, expected_root, expected_implicit, expected_depth) in [
            ("*.nt", ".", true, Some(1)),
            ("**/*.nt", ".", true, None),
            ("examples/*.nt", "examples", false, Some(1)),
            ("examples/**/*.nt", "examples", false, None),
            ("a/b/*/c.nt", "a/b", false, Some(2)),
            ("/tmp/data/*.ttl", "/tmp/data", false, Some(1)),
            ("literal.nt", "literal.nt", false, Some(0)),
        ] {
            let (root, implicit, depth) = plan_walk(pattern);
            assert_eq!(root, Path::new(expected_root), "root of {pattern}");
            assert_eq!(implicit, expected_implicit, "implicit root of {pattern}");
            assert_eq!(depth, expected_depth, "depth of {pattern}");
        }
    }
}
