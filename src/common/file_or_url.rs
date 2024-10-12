use std::sync::LazyLock;

use anyhow::{Error, Result};
use regex::Regex;
use reqwest::Url;

#[derive(Clone, Debug)]
pub enum FileOrUrl {
    File(String),
    Url(Url),
    StdIn,
}

impl std::str::FromStr for FileOrUrl {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        static URL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new("^http(s)?://").unwrap());
        if value == "-" {
            Ok(FileOrUrl::StdIn)
        } else if URL_RE.is_match(value) {
            Ok(FileOrUrl::Url(Url::parse(value)?))
        } else if std::fs::exists(value)? {
            Ok(FileOrUrl::File(value.to_string()))
        } else {
            Err(Error::msg(format!(
                "Neither an http(s) URL nor an existing file: {value}"
            )))
        }
    }
}

impl std::fmt::Display for FileOrUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            FileOrUrl::File(s) => s,
            FileOrUrl::Url(s) => s.as_str(),
            FileOrUrl::StdIn => "-",
        };
        txt.fmt(f)
    }
}
