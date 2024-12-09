use std::{iter::once, path::PathBuf, sync::LazyLock};

use anyhow::{Error, Result};
use glob::Pattern;
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

    type IntoIter = Box<dyn Iterator<Item = PathOrUrl>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            FilesOrUrl::File(filename) => Box::new(once(PathOrUrl::Path(filename.into()))),
            FilesOrUrl::Glob(pattern) => {
                let mut paths = glob::glob(pattern.as_str()).expect("pattern is valid");
                match paths.next() {
                    None => {
                        log::warn!("Pattern '{}' matches no file", pattern.as_str());
                        Box::new(std::iter::empty())
                    }
                    Some(first) => Box::new(once(first).chain(paths).filter_map(
                        |res| -> Option<PathOrUrl> {
                            match res {
                                Err(err) => {
                                    log::warn!("GlobError: {err}");
                                    None
                                }
                                Ok(path) => Some(PathOrUrl::Path(path)),
                            }
                        },
                    )),
                }
            }
            FilesOrUrl::Url(url) => Box::new(once(PathOrUrl::Url(url))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum PathOrUrl {
    Path(PathBuf),
    Url(Url),
}
