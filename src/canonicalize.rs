use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::Error;
use anyhow::Result;
use sophia::api::quad::Spog;
use sophia::api::source::QuadSource;
use sophia::api::term::SimpleTerm;
use sophia::c14n::{
    self,
    hash::{Sha256, Sha384},
    rdfc10::{DEFAULT_DEPTH_FACTOR, DEFAULT_PERMUTATION_LIMIT},
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
    #[arg(short, long)]
    output: Option<String>,

    /// Canonicalization function to use (supported: RDFC-1.0, Sophia-C14N)
    #[arg(
        short,
        long,
        default_value_t = C14nFunction::RDFC10,
    )]
    function: C14nFunction,

    /// Hash function to use (supported: SHA-256, SHA-384)
    ///
    /// Default depends on c14n function; some c14n function may not support
    /// all hash functions.
    #[arg(
        short = 'H',
        long,
        default_value_t = HashFunctionId::Sha256,
        verbatim_doc_comment
    )]
    hash_function: HashFunctionId,

    /// Resistance to "poison graphs"
    ///
    /// Higher value means that the c14n will stop earlier when complex
    /// graphs are encountered.
    #[arg(short, long, default_value_t = FiniteNonNegativeF64(1.0), verbatim_doc_comment)]
    poison_resistance: FiniteNonNegativeF64,
}

pub fn run(quads: QuadIter, mut args: Args) -> Result<()> {
    log::trace!("canonicalize args: {args:#?}");
    let dataset: MyDataset = quads.collect_quads()?;
    match args.output.take() {
        None => run_with_output(dataset, args, stdout()),
        Some(filename) => run_with_output(dataset, args, File::create(filename)?),
    }
}

fn run_with_output<W: Write>(dataset: MyDataset, args: Args, output: W) -> Result<()> {
    let output = BufWriter::new(output);
    let poison_resistance: f64 = args.poison_resistance.into();
    let hash = args.hash_function;
    match args.function {
        C14nFunction::RDFC10 => run_rdfc10(dataset, output, poison_resistance, hash),
        C14nFunction::Sophia => run_sophia(dataset, output, poison_resistance, hash),
    }
}

fn run_rdfc10<W: Write>(
    dataset: MyDataset,
    output: BufWriter<W>,
    poison_resistance: f64,
    hash: HashFunctionId,
) -> Result<()> {
    let depth_factor = DEFAULT_DEPTH_FACTOR * poison_resistance as f32;
    let permutation_limit = (DEFAULT_PERMUTATION_LIMIT as f64 * poison_resistance) as usize;
    match hash {
        HashFunctionId::Sha256 => c14n::rdfc10::normalize_with::<Sha256, _, _>(
            &dataset,
            output,
            depth_factor,
            permutation_limit,
        )?,
        HashFunctionId::Sha384 => c14n::rdfc10::normalize_with::<Sha384, _, _>(
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

fn run_sophia<W: Write>(
    dataset: MyDataset,
    output: BufWriter<W>,
    poison_resistance: f64,
    hash: HashFunctionId,
) -> Result<()> {
    let depth_factor = DEFAULT_DEPTH_FACTOR * poison_resistance as f32;
    let permutation_limit = (DEFAULT_PERMUTATION_LIMIT as f64 * poison_resistance) as usize;
    match hash {
        HashFunctionId::Sha256 => c14n::sophia::normalize_with::<Sha256, _, _>(
            &dataset,
            output,
            depth_factor,
            permutation_limit,
        )?,
        HashFunctionId::Sha384 => c14n::sophia::normalize_with::<Sha384, _, _>(
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
