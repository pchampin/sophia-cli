use clap::Parser;

#[derive(clap::Subcommand, Clone, Debug)]
#[command(
    subcommand_value_name = "PIPELINE",
    subcommand_help_heading = "Pipeline",
    disable_help_subcommand = true
)]
pub enum PipeSubcommand {
    // // NB: this would be ideal, but causes a stack overflow
    // /// Optionally pipe quads to another subcommand
    // #[command(subcommand, name="!")]
    // Pipe(Box<crate::SinkSubcommand>)

    // // NB: instead, we defer the parsing of the piped command
    /// Optionanally pipe quads to another subcommand
    #[command(name = "!")]
    Pipe(PipeArgs),
}

#[derive(clap::Args, Clone, Debug)]
pub struct PipeArgs {
    #[arg(trailing_var_arg = true)]
    args: Vec<String>,
}

impl PipeSubcommand {
    pub fn parse(&self) -> crate::SinkSubcommand {
        let PipeSubcommand::Pipe(pipe) = self;
        PipeCommand::parse_from(&pipe.args[..]).subcommand
    }
}

// Only used for parsing, never used as a
#[derive(Parser, Clone, Debug)]
#[command(name = "!", multicall = true, disable_help_subcommand = true)]
pub struct PipeCommand {
    #[command(subcommand)]
    subcommand: crate::SinkSubcommand,
}
