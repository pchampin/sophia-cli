use std::convert::Infallible;

use anyhow::{bail, Context, Error, Result};
use sophia::{
    api::{
        ns::xsd,
        quad::Spog,
        source::{QuadSource, TripleSource},
        sparql::{SparqlDataset, SparqlResult},
        term::Term,
    },
    reasoner::{
        d_entailment::{self, Recognized},
        dataset::ReasonableDataset,
        ruleset::{self, RuleSet},
    },
    sparql::{Bindings, ResultTerm, SparqlWrapper, SparqlWrapperError},
    term::ArcTerm,
};

use crate::common::{
    pipe::PipeSubcommand,
    quad_handler::QuadHandler,
    quad_iter::{QuadIter, QuadIterError},
};

/// Execute a SPARQL query against the quads
///
/// The result can be piped to subcommands if
/// - the query is a CONSTRUCT or a DESCRIBED query, or
/// - the query is a SELECT query with variables ?s, ?p, ?o,
///   and (optionally) ?g.
#[derive(clap::Args, Clone, Debug)]
#[command(verbatim_doc_comment)]
pub struct Args {
    /// SPARQL query
    #[arg()]
    query: String,

    /// Entailement regime to apply, one of 'simple', 'rdf' or 'rdfs' (defaults to 'simple')
    #[arg(short = 'r', default_value = "simple")]
    reasoning: EntailmentRegime,

    /// Whether common datatypes must be *recognized* (as defined by RDF 1.2 Semantics).
    ///
    /// When a datatype is recognized,
    /// literals of that datatype are treated independently of their syntactic representation.
    /// For example, `42` and `042` will be considered *identical* (not just equal).
    ///
    /// The datatypes recognized when this option is enabled are:
    ///
    /// - all datatypes required by RDF semantics: rdf:langString, rdf:dirLangString xsd:string
    ///
    /// - all datatypes required by SPARQL Query: xsd:integer xsd:decimal xsd:float xsd:double xsd:string xsd:boolean xsd:dateTime xsd:nonPositiveInteger xsd:negativeInteger xsd:long xsd:int xsd:short xsd:byte xsd:nonNegativeInteger xsd:unsignedLong xsd:unsignedInt xsd:unsignedShort xsd:unsignedByte xsd:positiveInteger
    ///
    /// More datatypes may be supported in the future.
    #[arg(short = 'd')]
    datatypes: bool,

    /// Do not output column headers (variable names) for bindings
    ///
    /// This flag is ignored if query is not SELECT.
    #[arg(short = 'H', long)]
    no_headers: bool,

    /// Exit with an error status if boolean result is `false` (ASK only)
    ///
    /// The result of the query will also not be printed to the output.
    /// This flag is ignored if query is not ASK.
    #[arg(short, long)]
    status: bool,

    #[command(subcommand)]
    pipeline: Option<PipeSubcommand>,
}

pub fn run(quads: QuadIter, args: Args) -> Result<()> {
    log::trace!("query args: {args:#?}");
    if args.datatypes {
        run_with_d::<d_entailment::Sparql>(quads, args)
    } else {
        run_with_d::<d_entailment::Nothing>(quads, args)
    }
}

pub fn run_with_d<D: Recognized>(quads: QuadIter, args: Args) -> Result<()> {
    match args.reasoning {
        EntailmentRegime::Simple => run_with_d_r::<D, ruleset::Simple>(quads, args),
        EntailmentRegime::Rdf => run_with_d_r::<D, ruleset::Rdf>(quads, args),
        EntailmentRegime::Rdfs => run_with_d_r::<D, ruleset::Rdfs>(quads, args),
    }
}

pub fn run_with_d_r<D: Recognized, R: RuleSet>(quads: QuadIter, args: Args) -> Result<()> {
    let dataset: ReasonableDataset<D, R> = quads.collect_quads()?;
    let sparql = SparqlWrapper(&dataset);
    let query = sparql
        .prepare_query(&args.query[..])
        .context("SPARQL parse error")?;
    log::debug!("{:#?}", query);
    match sparql.query(&query).context("SPARQL eval error")? {
        SparqlResult::Bindings(bindings) => handle_bindings(bindings, args)?,
        SparqlResult::Boolean(response) => handle_boolean(response, args)?,
        SparqlResult::Triples(triples) => handle_triples(triples, args)?,
    };
    Ok(())
}

fn handle_bindings<D: Recognized, R: RuleSet>(
    bindings: Bindings<ReasonableDataset<D, R>>,
    args: Args,
) -> Result<()> {
    let vars = bindings.variables();
    if let Some(pipeline) = args.pipeline {
        // TODO combine the check and the extraction on indices
        let Some(extractor) = QuadExtractor::try_new(&vars) else {
            bail!("Can only pipe bindings to sub-command if variables are ?s, ?p, ?o and optionally ?g.")
        };
        let handler = QuadHandler::new(Some(pipeline));
        handler.handle_quads(QuadIter::new(bindings.into_iter().filter_map(
            |res| match res {
                Ok(b) => Ok(extractor.extract(b)).transpose(),
                Err(err) => Some(Err(QuadIterError::new(err))),
            },
        )))
    } else {
        if !args.no_headers && !vars.is_empty() {
            println!("?{}", vars.join("\t?"));
        }

        let mut seps = vec!["\t"; vars.len()];
        if !vars.is_empty() {
            seps[vars.len() - 1] = "";
        }

        for res in bindings {
            for (opt, sep) in res?.into_iter().zip(&seps) {
                if let Some(value) = opt {
                    pretty_print(value);
                }
                print!("{sep}");
            }
            println!();
        }
        Ok(())
    }
}

fn pretty_print(term: ResultTerm) {
    if let Some(dt) = term.datatype() {
        let lex = term.lexical_form().unwrap();
        if xsd::string == dt {
            print!("{lex:?}");
            return;
        }
        if xsd::boolean == dt || xsd::decimal == dt || xsd::double == dt || xsd::integer == dt {
            print!("{lex}");
            return;
        }
    }
    print!("{term}");
}

fn handle_boolean(response: bool, args: Args) -> Result<()> {
    if args.pipeline.is_some() {
        bail!("Can not pipe boolean result to sub-command")
    } else if args.status {
        std::process::exit(if response { 0 } else { 128 })
    } else {
        println!("{response}");
        Ok(())
    }
}

fn handle_triples(
    triples: impl Iterator<Item = Result<[ResultTerm; 3], SparqlWrapperError<Infallible>>>,
    args: Args,
) -> Result<()> {
    let handler = QuadHandler::new(args.pipeline);
    handler.handle_quads(QuadIter::from_quad_source(
        triples.map_triples(|spo| (spo, None)),
    ))?;
    Ok(())
}

struct QuadExtractor {
    is: usize,
    ip: usize,
    io: usize,
    ig: Option<usize>,
}

impl QuadExtractor {
    fn try_new(variables: &[&str]) -> Option<Self> {
        if variables.len() < 3 || variables.len() > 4 {
            return None;
        }
        let [mut is, mut ip, mut io, mut ig] = [None; 4];
        for (i, v) in variables.iter().enumerate() {
            match *v {
                "s" => is = Some(i),
                "p" => ip = Some(i),
                "o" => io = Some(i),
                "g" => ig = Some(i),
                _ => return None,
            }
        }
        Some(Self {
            is: is?,
            ip: ip?,
            io: io?,
            ig,
        })
    }

    fn extract(&self, mut b: Vec<Option<ResultTerm>>) -> Option<Spog<ArcTerm>> {
        Some((
            [
                b[self.is].take()?.unwrap(),
                b[self.ip].take()?.unwrap(),
                b[self.io].take()?.unwrap(),
            ],
            self.ig.and_then(|ig| b[ig].take().map(ResultTerm::unwrap)),
        ))
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum EntailmentRegime {
    #[default]
    Simple,
    Rdf,
    Rdfs,
}

impl std::str::FromStr for EntailmentRegime {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "simple" => Ok(Self::Simple),
            "rdf" => Ok(Self::Rdf),
            "rdfs" => Ok(Self::Rdfs),
            _ => Err(Error::msg(format!(
                "Unrecognized entailmement regime: {s:?}"
            ))),
        }
    }
}
