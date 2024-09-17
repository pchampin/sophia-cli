use std::fs::File;
use std::io::{stdout, BufWriter};

use anyhow::Error;
use anyhow::Result;
use sophia::api::quad::Spog;
use sophia::api::source::QuadSource;
use sophia::api::term::SimpleTerm;
use sophia::c14n::rdfc10::{DEFAULT_DEPTH_FACTOR, DEFAULT_PERMUTATION_LIMIT};
use sophia::c14n::{
    hash::{HashFunction, Sha256, Sha384},
    rdfc10,
};

use crate::common::f64::FiniteNonNegativeF64;

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

pub fn run<Q: QuadSource>(quads: Q, args: Args) -> Result<()>
where
    <Q as QuadSource>::Error: Send + Sync,
{
    log::trace!("canonicalize args: {args:#?}");
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
    match hash {
        HashFunctionId::Sha256 => {
            run_rdfc10_with::<Sha256>(dataset, args.output, depth_factor, permutation_limit)?
        }
        HashFunctionId::Sha384 => {
            run_rdfc10_with::<Sha384>(dataset, args.output, depth_factor, permutation_limit)?
        }
        #[allow(unreachable_patterns)]
        _ => Err(Error::msg("Cannot apply RDFC-10 with hash function {hash}"))?,
    }
    Ok(())
}

fn run_rdfc10_with<H: HashFunction>(
    dataset: MyDataset,
    output: Option<String>,
    depth_factor: f32,
    permutation_limit: usize,
) -> Result<()> {
    match output {
        None => rdfc10::normalize_with::<H, _, _>(
            &dataset,
            BufWriter::new(stdout()),
            depth_factor,
            permutation_limit,
        )?,
        Some(filename) => rdfc10::normalize_with::<H, _, _>(
            &dataset,
            BufWriter::new(File::create(filename)?),
            depth_factor,
            permutation_limit,
        )?,
    }
    Ok(())
}

type MyDataset = std::collections::HashSet<Spog<SimpleTerm<'static>>>;
