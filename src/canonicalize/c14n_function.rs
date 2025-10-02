use std::{str::FromStr, sync::LazyLock};

use anyhow::Error;
use regex::{RegexSet, RegexSetBuilder};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum C14nFunction {
    RDFC10,
    Sophia,
}

impl FromStr for C14nFunction {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        static RES: LazyLock<RegexSet> = LazyLock::new(|| {
            RegexSetBuilder::new([r"^( RDFC-?(1(\.?0)?)? )$", r"^( Sophia(-C14N)? )$"])
                .ignore_whitespace(true)
                .case_insensitive(true)
                .build()
                .unwrap()
        });
        match RES.matches(s).iter().next() {
            Some(0) => Ok(C14nFunction::RDFC10),
            Some(1) => Ok(C14nFunction::Sophia),
            _ => Err(Error::msg(format!("Unrecognized c14n function {s}"))),
        }
    }
}

impl std::fmt::Display for C14nFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            C14nFunction::RDFC10 => write!(f, "RDFC-1.0"),
            C14nFunction::Sophia => write!(f, "Sophia-C14N"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    use C14nFunction::*;

    #[test_case("RDFC" => RDFC10)]
    #[test_case("RDFC1" => RDFC10)]
    #[test_case("RDFC10" => RDFC10)]
    #[test_case("RDFC1.0" => RDFC10; "RDFC1dot0")]
    #[test_case("RDFC-1" => RDFC10)]
    #[test_case("RDFC-10" => RDFC10)]
    #[test_case("RDFC-1.0" => RDFC10; "RDFC-1dot0")]
    #[test_case("rdfc10" => RDFC10; "rdfc10 lower")]
    fn c14n_function(txt: &str) -> C14nFunction {
        txt.parse().unwrap()
    }
}
