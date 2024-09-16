use std::str::FromStr;

use anyhow::{Error, Result};

#[derive(Clone, Copy, Debug)]
pub struct FiniteNonNegativeF64(pub(crate) f64);

impl FiniteNonNegativeF64 {
    pub fn new_unchecked(value: f64) -> Self {
        debug_assert!(Self::try_from(value).is_ok());
        Self(value)
    }
}

impl TryFrom<f64> for FiniteNonNegativeF64 {
    type Error = Error;

    fn try_from(value: f64) -> Result<Self> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(Error::msg(format!("{value} is not finite and positive")))
        }
    }
}

impl FromStr for FiniteNonNegativeF64 {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        s.parse::<f64>()?.try_into()
    }
}

impl std::fmt::Display for FiniteNonNegativeF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<FiniteNonNegativeF64> for f64 {
    fn from(wrapper: FiniteNonNegativeF64) -> Self {
        wrapper.0
    }
}

impl From<FiniteNonNegativeF64> for f32 {
    fn from(wrapper: FiniteNonNegativeF64) -> Self {
        wrapper.0 as f32
    }
}
