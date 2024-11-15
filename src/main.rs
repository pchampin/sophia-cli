use anyhow::Result;
use clap::Parser;
use clap_verbosity::InfoLevel;
use sophia::api::source::QuadSource;

mod canonicalize;
mod common;
mod merge_default_graph;
mod parse;
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

/// Subcommands that can only be used on the left-hand side of a pipe
#[derive(clap::Subcommand, Clone, Debug)]
enum SourceSubcommand {
    #[command(visible_aliases=["p"], aliases=["pa", "par"])]
    Parse(parse::Args),
}

/// Subcommands that can be used on the right-hand side of a pipe
#[derive(clap::Subcommand, Clone, Debug)]
enum SinkSubcommand {
    #[command(visible_aliases=["c", "c14n"], aliases=["ca", "can"])]
    Canonicalize(canonicalize::Args),
    #[command(visible_aliases=["s"], aliases=["se", "ser"])]
    Serialize(serialize::Args),
    #[command(visible_aliases=["m"], aliases=["me", "mer"])]
    MergeDefaultGraph(merge_default_graph::Args),
}

impl SinkSubcommand {
    pub fn handle_quads<Q: QuadSource>(self, quads: Q) -> Result<()>
    where
        <Q as QuadSource>::Error: Send + Sync,
    {
        match self {
            Self::Canonicalize(args) => canonicalize::run(quads, args),
            Self::Serialize(args) => serialize::run(quads, args),
            Self::MergeDefaultGraph(args) => merge_default_graph::run(quads, args),
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

fn quad_from_stdin() -> impl QuadSource<Error = rio_turtle::TurtleError> {
    sophia::turtle::parser::nq::parse_bufread(std::io::BufReader::new(std::io::stdin()))
}
