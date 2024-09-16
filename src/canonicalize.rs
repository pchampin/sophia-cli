use std::fs::File;
use std::io::{stdout, BufWriter};

use anyhow::{Context, Error, Result};
use sophia::api::dataset::Dataset;
use sophia::api::quad::Spog;
use sophia::api::source::QuadSource;
use sophia::api::term::SimpleTerm;
use sophia::c14n::rdfc10::{DEFAULT_DEPTH_FACTOR, DEFAULT_PERMUTATION_LIMIT};
use sophia::c14n::{
    hash::{HashFunction, Sha256, Sha384},
    rdfc10,
};

use crate::common::f64::FiniteNonNegativeF64;
use crate::common::pipe::PipeSubcommand;

mod c14n_function;
use c14n_function::*;
mod hash_function;
use hash_function::*;

/// Serialize quads to a canonical form
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// File to serialize into [default: standard output]
    ///
    /// Cannot be used with a pipeline.
    #[arg(short, long)]
    output: Option<String>,

    /// C14n function to use (supported: RDFC-1.0)
    #[arg(
        short,
        long,
        default_value_t = C14nFunction::RDFC10,
        hide_default_value = true, // since there is only one possible value for now
    )]
    function: C14nFunction,

    /// Hash function to use (supported: SHA-256, SHA-384).
    ///
    /// Default depends on c14n function; some c14n function may not support all hash function.
    #[arg(short = 'H', long)]
    hash_function: Option<HashFunctionId>,

    /// Higher value means that the c14n will stop earlier when complex graphs are encountered.
    #[arg(short, long, default_value_t = FiniteNonNegativeF64(1.0))]
    poison_resistance: FiniteNonNegativeF64,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run<Q: QuadSource>(quads: Q, args: Args) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
{
    log::trace!("canonicalize args: {args:#?}");
    if args.output.is_some() && args.pipeline.is_some() {
        Err(Error::msg(
            "Cannot use --output and pipeline at the same time",
        ))?
    }
    let dataset: MyDataset = quads.collect_quads()?;

    match args.function {
        C14nFunction::RDFC10 => run_rdfc10(dataset, args),
    }
}

fn run_rdfc10(dataset: MyDataset, args: Args) -> Result<()> {
    let hash = args.hash_function.unwrap_or(HashFunctionId::Sha256);
    let poison_resistance: f64 = args.poison_resistance.into();
    let depth_factor = DEFAULT_DEPTH_FACTOR * poison_resistance as f32;
    let permutation_limit = (DEFAULT_PERMUTATION_LIMIT as f64 * poison_resistance) as usize;
    let handler = HybridHandler::new(args.pipeline, args.output)?;
    match hash {
        HashFunctionId::Sha256 => {
            run_rdfc10_with::<Sha256>(dataset, handler, depth_factor, permutation_limit)?
        }
        HashFunctionId::Sha384 => {
            run_rdfc10_with::<Sha384>(dataset, handler, depth_factor, permutation_limit)?
        }
        #[allow(unreachable_patterns)]
        _ => Err(Error::msg("Cannot apply RDFC-10 with {hash}"))?,
    }
    Ok(())
}

fn run_rdfc10_with<H: HashFunction>(
    dataset: MyDataset,
    handler: HybridHandler,
    depth_factor: f32,
    permutation_limit: usize,
) -> Result<()> {
    use HybridHandler::*;
    match handler {
        Stdout => rdfc10::normalize_with::<H, _, _>(
            &dataset,
            BufWriter::new(stdout()),
            depth_factor,
            permutation_limit,
        )?,
        File(f) => rdfc10::normalize_with::<H, _, _>(
            &dataset,
            BufWriter::new(f),
            depth_factor,
            permutation_limit,
        )?,
        Pipeline(sink) => {
            let (ds, _) = rdfc10::relabel_with::<H, _>(&dataset, depth_factor, permutation_limit)?;
            sink.handle_quads(ds.quads())?;
        }
    }
    Ok(())
}

type MyDataset = std::collections::HashSet<Spog<SimpleTerm<'static>>>;

enum HybridHandler {
    Stdout,
    File(File),
    Pipeline(crate::SinkSubcommand),
}

impl HybridHandler {
    pub fn new(pipeline: Option<PipeSubcommand>, output: Option<String>) -> Result<Self> {
        debug_assert!(pipeline.is_none() || output.is_none());
        if let Some(pipe) = pipeline {
            pipe.try_parse()
                .map(Self::Pipeline)
                .with_context(|| "Error parsing subcommand in pipeline")
        } else if let Some(output) = output {
            Ok(Self::File(File::create(output)?))
        } else {
            Ok(Self::Stdout)
        }
    }
}
