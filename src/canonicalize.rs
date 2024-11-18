use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::Error;
use anyhow::Result;
use sophia::api::quad::Spog;
use sophia::api::source::QuadSource;
use sophia::api::term::SimpleTerm;
use sophia::c14n::rdfc10::{DEFAULT_DEPTH_FACTOR, DEFAULT_PERMUTATION_LIMIT};
use sophia::c14n::{
    hash::{Sha256, Sha384},
    rdfc10,
};

use crate::common::f64::FiniteNonNegativeF64;
use crate::common::quad_iter::QuadIter;

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
}

pub fn run(mut quads: QuadIter, mut args: Args) -> Result<()> {
    log::trace!("canonicalize args: {args:#?}");
    let dataset: MyDataset = quads.collect_quads()?;
    match args.output.take() {
        None => run_with_output(dataset, args, stdout()),
        Some(filename) => run_with_output(dataset, args, File::create(filename)?),
    }
}

fn run_with_output<W: Write>(dataset: MyDataset, args: Args, output: W) -> Result<()> {
    let output = BufWriter::new(output);
    match args.function {
        C14nFunction::RDFC10 => run_rdfc10(dataset, output, args),
    }
}

fn run_rdfc10<W: Write>(dataset: MyDataset, output: BufWriter<W>, args: Args) -> Result<()> {
    let hash = args.hash_function.unwrap_or(HashFunctionId::Sha256);
    let poison_resistance: f64 = args.poison_resistance.into();
    let depth_factor = DEFAULT_DEPTH_FACTOR * poison_resistance as f32;
    let permutation_limit = (DEFAULT_PERMUTATION_LIMIT as f64 * poison_resistance) as usize;
    match hash {
        HashFunctionId::Sha256 => rdfc10::normalize_with::<Sha256, _, _>(
            &dataset,
            output,
            depth_factor,
            permutation_limit,
        )?,
        HashFunctionId::Sha384 => rdfc10::normalize_with::<Sha384, _, _>(
            &dataset,
            output,
            depth_factor,
            permutation_limit,
        )?,
        #[allow(unreachable_patterns)]
        _ => Err(Error::msg("Cannot apply RDFC-10 with hash function {hash}"))?,
    }
    Ok(())
}

type MyDataset = std::collections::HashSet<Spog<SimpleTerm<'static>>>;
