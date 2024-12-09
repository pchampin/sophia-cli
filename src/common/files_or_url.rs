use std::{mem::take, path::PathBuf, sync::LazyLock};

use anyhow::{Error, Result};
use glob::{GlobError, Paths, Pattern};
use regex::Regex;
use reqwest::Url;

#[derive(Clone, Debug)]
pub enum FilesOrUrl {
    File(String),
    Glob(Pattern),
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
        } else if let Ok(pattern) = Pattern::new(value) {
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
            FilesOrUrl::Glob(pattern) => {
                let mut paths = glob::glob(pattern.as_str()).expect("pattern is valid");
                match paths.next() {
                    None => {
                        log::warn!("Pattern '{}' matches no file", pattern.as_str());
                        IntoIter::End
                    }
                    Some(first) => IntoIter::GlobFirst(first, paths),
                }
            }
            FilesOrUrl::Url(url) => IntoIter::Url(url),
        }
    }
}

pub enum IntoIter {
    End,
    File(PathBuf),
    GlobFirst(Result<PathBuf, GlobError>, Paths),
    GlobRest(Paths),
    Url(Url),
}

impl Default for IntoIter {
    fn default() -> Self {
        Self::End
    }
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
            Self::GlobFirst(..) => {
                let Self::GlobFirst(first, rest) = take(self) else {
                    unreachable!()
                };
                *self = Self::GlobRest(rest);
                match first {
                    Ok(first) => Some(PathOrUrl::Path(first)),
                    Err(err) => {
                        log::warn!("{err}");
                        self.next()
                    }
                }
            }
            Self::GlobRest(rest) => match rest.next()? {
                Ok(path) => Some(PathOrUrl::Path(path)),
                Err(err) => {
                    log::warn!("{err}");
                    self.next()
                }
            },
            Self::Url(_) => {
                let Self::Url(url) = take(self) else {
                    unreachable!()
                };
                Some(PathOrUrl::Url(url))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum PathOrUrl {
    Path(PathBuf),
    Url(Url),
}
