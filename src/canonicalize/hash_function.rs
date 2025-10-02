use std::{str::FromStr, sync::LazyLock};

use anyhow::Error;
use regex::{RegexSet, RegexSetBuilder};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashFunctionId {
    Sha256,
    Sha384,
}

impl FromStr for HashFunctionId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        static RES: LazyLock<RegexSet> = LazyLock::new(|| {
            RegexSetBuilder::new([r"^( SHA-?256 )$", r"^( SHA-?384 )$"])
                .ignore_whitespace(true)
                .case_insensitive(true)
                .build()
                .unwrap()
        });
        match RES.matches(s).iter().next() {
            Some(0) => Ok(HashFunctionId::Sha256),
            Some(1) => Ok(HashFunctionId::Sha384),
            _ => Err(Error::msg(format!("Unrecognized hash function {s}"))),
        }
    }
}

impl std::fmt::Display for HashFunctionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashFunctionId::Sha256 => write!(f, "SHA-256"),
            HashFunctionId::Sha384 => write!(f, "SHA-384"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    use HashFunctionId::*;

    #[test_case("Sha256" => Sha256)]
    #[test_case("Sha-256" => Sha256)]
    #[test_case("SHA256" => Sha256; "sha256 upper")]
    #[test_case("SHA-256" => Sha256; "sha-256 upper")]
    #[test_case("sha256" => Sha256; "sha256lower")]
    #[test_case("Sha384" => Sha384)]
    #[test_case("Sha-384" => Sha384)]
    #[test_case("SHA384" => Sha384; "sha384 upper")]
    #[test_case("SHA-384" => Sha384; "sha-384 upper")]
    #[test_case("sha384" => Sha384; "sha384lower")]
    fn hash_function(txt: &str) -> HashFunctionId {
        txt.parse().unwrap()
    }

    #[test_case(Sha256)]
    #[test_case(Sha384)]
    fn display_round_trip(h: HashFunctionId) {
        assert_eq!(h, h.to_string().parse::<HashFunctionId>().unwrap())
    }
}
