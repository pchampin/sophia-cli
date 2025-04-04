use anyhow::Result;
use clap::Parser;
use clap_verbosity::InfoLevel;
use common::quad_iter::QuadIter;

mod absolutize;
mod canonicalize;
mod common;
mod dispatch;
mod filter;
mod merge;
mod null;
mod parse;
mod query;
mod relativize;
mod serialize;

/// Swiss-army knife for processing RDF and Linked Data.
#[derive(Parser, Debug)]
#[command(version, about, disable_help_subcommand = true)]
struct CmdArgs {
    #[command(flatten)]
    verbose: common::verbosity::Verbosity<InfoLevel>,

    #[command(subcommand)]
    pub subcommand: Subcommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum Subcommand {
    #[command(flatten)]
    Source(SourceSubcommand),

    #[command(flatten)]
    Sink(SinkSubcommand),
}

/// Subcommands that can only be used on the left-hand side of a pipe (or on their own)
#[derive(clap::Subcommand, Clone, Debug)]
enum SourceSubcommand {
    #[command(visible_aliases=["p"], aliases=["pa", "par"])]
    Parse(parse::Args),
}

/// Subcommands that can be used on the right-hand side of a pipe
#[derive(clap::Subcommand, Clone, Debug)]
enum SinkSubcommand {
    #[command(visible_aliases=["a"], aliases=["ab", "abs"])]
    Absolutize(absolutize::Args),
    #[command(visible_aliases=["c", "c14n"], aliases=["ca", "can"])]
    Canonicalize(canonicalize::Args),
    #[command(visible_aliases=["d"], aliases=["di", "dis"])]
    Dispatch(dispatch::Args),
    #[command(visible_aliases=["f"], aliases=["fi", "fil"])]
    Filter(filter::Args),
    #[command(visible_aliases=["m", "merge-default-graph"], aliases=["me", "mer"])]
    Merge(merge::Args),
    #[command(visible_aliases=["q"], aliases=["qu", "que"])]
    Query(query::Args),
    #[command(visible_aliases=["r"], aliases=["re", "rel"])]
    Relativize(relativize::Args),
    #[command(visible_aliases=["s"], aliases=["se", "ser"])]
    Serialize(serialize::Args),
    #[command(visible_aliases=["n", "Z"], aliases=["nu", "nul"])]
    Null(null::Args),
}

impl SinkSubcommand {
    pub fn handle_quads(self, quads: QuadIter) -> Result<()> {
        match self {
            Self::Absolutize(args) => absolutize::run(quads, args),
            Self::Canonicalize(args) => canonicalize::run(quads, args),
            Self::Dispatch(args) => dispatch::run(quads, args),
            Self::Filter(args) => filter::run(quads, args),
            Self::Merge(args) => merge::run(quads, args),
            Self::Query(args) => query::run(quads, args),
            Self::Relativize(args) => relativize::run(quads, args),
            Self::Serialize(args) => serialize::run(quads, args),
            Self::Null(args) => null::run(quads, args),
        }
    }
}

fn main() -> Result<()> {
    let args = CmdArgs::parse();

    env_logger::builder()
        .format_timestamp(None)
        .filter_level(args.verbose.log_level_filter())
        .init();
    use SourceSubcommand::*;
    use Subcommand::*;
    match args.subcommand {
        Source(Parse(args)) => parse::run(args),
        Sink(sink) => sink.handle_quads(quad_from_stdin()),
    }
}

fn quad_from_stdin() -> QuadIter<'static> {
    QuadIter::from_quad_source(sophia::turtle::parser::gnq::parse_bufread(
        std::io::BufReader::new(std::io::stdin()),
    ))
}
