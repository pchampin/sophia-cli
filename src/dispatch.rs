use std::{collections::HashSet, fs::File, path::Path};

use anyhow::Result;
use sophia::{
    api::{
        prelude::{Any, Dataset},
        source::QuadSource,
        term::{FromTerm, SimpleTerm, Term},
    },
    inmem::dataset::LightDataset,
    iri::{is_absolute_iri_ref, relativize::Relativizer, Iri},
};

use crate::{
    common::{
        format::Format,
        pipe::PipeSubcommand,
        quad_handler::QuadHandler,
        quad_iter::{quad_iter_item, QuadIter},
    },
    relativize::RelativizerExt,
    serialize::{self, SerializerArgs, SerializerOptions},
};

/// Dispatch quads onto the filesystem based on their graph name
///
/// This command expects a root IRI.
/// All and only graph names starting with the root IRI will be dispatched,
/// i.e. written into a file whose path is the relative path of the graph name
/// w.r.t. to the root IRI.
///
/// E.g., if the root is <https://example.org/foo/>, the following named graphs
/// will be dispatched as follows:
/// * <https://example.org/foo/g1.ttl> → written to `./g1.ttl`
/// * <https://example.org/foo/bar/g2.ttl> → written to `./g2.ttl`
/// * <https://example.org/baz/g3.ttl> → not dispatched
/// * <https://another.domain/foo/g4.ttl> → not dispatched
///
/// Any quad that is not dispatched will be passed to the next command in the
/// pipeline (or serialized to stdout if there is no further command).
#[derive(clap::Args, Clone, Debug)]
pub struct Args {
    /// IRI to be used as root for dispatching
    #[arg(value_parser = |txt: &str| Iri::new(txt.to_string()), verbatim_doc_comment)]
    root: Iri<String>,

    /// Directory to which graphs are dispatched
    #[arg(short, long, default_value = ".")]
    destination: std::path::PathBuf,

    /// Whether to overwrite existing files
    #[arg(short, long, default_value = "false")]
    overwrite: bool,

    /// Format to serialize to (if it can not be guessed from filename)
    #[arg(short, long)]
    format: Option<Format>,

    /// Whether to relativize serialized IRIs against the graph name IRI in dispatched files.
    #[arg(short, long)]
    relativize: bool,

    #[command(flatten)]
    options: SerializerOptions,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, mut args: Args) -> Result<()> {
    log::trace!("dispatch args: {args:#?}");

    // sanitizing args
    if !args.root.ends_with("/") {
        let mut s = args.root.unwrap();
        s.push('/');
        args.root = Iri::new_unchecked(s);
    }
    let args = args; // don't need 'mut' anymore

    // dispatching appropriate graphs
    let dataset: LightDataset = quads.collect_quads()?;
    let graph_names = dataset
        .graph_names()
        .map(|res| res.map(SimpleTerm::from_term).map_err(anyhow::Error::from))
        .collect::<Result<HashSet<_>>>()?;
    for gn in graph_names {
        if let Some(path) = try_extract_path(&gn, &args.root) {
            do_dispatch(&dataset, &gn, path, &args)
                .inspect_err(|err| {
                    log::error!(
                        "Can not dispatch to {:?}: {err}",
                        args.destination.join(path)
                    )
                })
                .unwrap_or(());
        }
    }

    // pass all non-dispatched quads forward
    let handler = QuadHandler::new(args.pipeline);
    let not_dispatched = |g: Option<SimpleTerm>| -> bool {
        g.map(|gn| try_extract_path(&gn, &args.root).is_none())
            .unwrap_or(true)
    };
    handler.handle_quads(QuadIter::new(
        dataset
            .quads_matching(Any, Any, Any, not_dispatched)
            .map(quad_iter_item),
    ))
}

fn try_extract_path<'a>(gn: &'a SimpleTerm, root: &str) -> Option<&'a str> {
    if let SimpleTerm::Iri(iri) = gn {
        iri.starts_with(root).then(|| &iri[root.len()..])
    } else {
        None
    }
}

fn do_dispatch(dataset: &LightDataset, gn: &SimpleTerm, path: &str, args: &Args) -> Result<()> {
    let quads = QuadIter::new(
        dataset
            .quads_matching(Any, Any, Any, [Some(gn)])
            .map_quads(|(_, spo)| (None, spo)) // drop graph names
            .into_iter()
            .map(quad_iter_item),
    );

    let quads = if args.relativize {
        debug_assert!(gn.is_iri() && is_absolute_iri_ref(gn.iri().unwrap().as_str()));
        // the above must be true, otherwise, we would not be dispatching
        let base = Iri::new_unchecked(gn.iri().unwrap().unwrap()).to_base();
        let parents = path.bytes().filter(|b| *b == b'/').count() as u8;
        Relativizer::new(base, parents).relativize_iter(quads)
    } else {
        quads
    };

    let ext = path.rsplit(".").next().unwrap();
    let format = ext
        .parse::<Format>()
        .ok()
        .or(args.format)
        .or(Some(Format::NTriples));
    let ser_args = serialize::Args {
        main: SerializerArgs {
            format,
            output: None,
        },
        options: args.options.clone(),
    };

    let dest = args.destination.join(path);
    ensure_dirs(&dest)?;
    let (file, ow) = if args.overwrite {
        let ow = dest.exists();
        (File::create(&dest)?, ow)
        // There is a tiny chance of race condition,
        // where the file is created *between* the call to `dest.exists` and the call to `File::create`.
        // In this case, the log message below will be "written" instead of "overwritten",
        // but the contract will not be broken (the user intended to overwrite anyway)
        // so this is deemed acceptable.
    } else {
        (File::create_new(&dest)?, false)
    };
    if ow {
        log::warn!("Overwriting {dest:?}");
    } else {
        log::info!("Writing {dest:?}");
    }
    log::trace!("in {:?}", format.unwrap());

    serialize::serialize_to_write(quads, ser_args, file)
}

fn ensure_dirs(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() && !dir.exists() {
            ensure_dirs(dir)?;
            log::trace!("creating {dir:?}");
            std::fs::create_dir(dir)?;
        }
    }
    Ok(())
}
