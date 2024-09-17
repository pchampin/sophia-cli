//! Copy of clap_verbosity::Verbosity,
//! only modified to *not* be a global option.
use clap_verbosity::level_value;
use clap_verbosity::ErrorLevel;
use clap_verbosity::LogLevel;

#[derive(clap::Args, Debug, Clone, Default)]
pub struct Verbosity<L: LogLevel = ErrorLevel> {
    #[arg(
        long,
        short = 'v',
        action = clap::ArgAction::Count,
        help = L::verbose_help(),
        long_help = L::verbose_long_help(),
    )]
    verbose: u8,

    #[arg(
        long,
        short = 'q',
        action = clap::ArgAction::Count,
        help = L::quiet_help(),
        long_help = L::quiet_long_help(),
        conflicts_with = "verbose",
    )]
    quiet: u8,

    #[arg(skip)]
    phantom: std::marker::PhantomData<L>,
}

impl<L: LogLevel> Verbosity<L> {
    /// Get the log level filter.
    pub fn log_level_filter(&self) -> log::LevelFilter {
        {
            let verbosity = self.verbosity();
            match verbosity {
                i8::MIN..=-1 => None,
                0 => Some(log::Level::Error),
                1 => Some(log::Level::Warn),
                2 => Some(log::Level::Info),
                3 => Some(log::Level::Debug),
                4..=i8::MAX => Some(log::Level::Trace),
            }
        }
        .map(|l| l.to_level_filter())
        .unwrap_or(log::LevelFilter::Off)
    }

    fn verbosity(&self) -> i8 {
        level_value(L::default()) - (self.quiet as i8) + (self.verbose as i8)
    }
}

use std::fmt;

impl<L: LogLevel> fmt::Display for Verbosity<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.verbosity())
    }
}
