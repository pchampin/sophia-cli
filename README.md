sop: Semantic Operation Pipeline
================================

`sop` aims to be a swiss-army knife for processing RDF and Linked Data on the command line.

`sop` stands for "Semantic Operation Pipeline",
or as a shortcut for [Sophia](https://github.com/pchampin/sophia_rs),
the library it is based on.

## Build from source

### With Docker

If you have a working Rust toolchain, you might prefer to build directly [with cargo](#with_cargo).

```bash
docker build -t sop_builder .
docker run --rm -v $(pwd):/outside sop_builder -c "cp /app/target/release/sop /outside"
```

### With Cargo

```bash
cargo build --release
cp target/release/sop .
```

or

```bash
cargo install --path .
```
to install it directly in your path.

### With Homebrew

See https://github.com/ktk/homebrew-sop (thanks to @ktk).


## The Pipeline Concept

`sop` works by building a **pipeline** of subcommands.
Data (as a stream of quads) is passed from one subcommand to the next using the `!` operator.

This is very similar to **functional programming** concepts found in languages like JavaScript:
- `filter` works like `Array.prototype.filter()`: it keeps only the quads that match the expression.
- `map` works like `Array.prototype.map()`: it transforms each quad into a new one.

You can chain multiple operations together to perform complex transformations.
Each step in the pipeline receives the quads produced by the *previous* step.

Example: swap subject and object for all triples, then filter for a specific predicate:
```bash
sop parse examples/sample.nt ! map -s "?o" -o "?s" ! filter "?p = <http://example.org/p>" ! serialize -f nt
```
### Sink subcommands

Some subcommands (e.g. `canonicalize`, `null` or some forms of `query`) do not produce quads,
so they can only appear at the end of the pipeline.

## Quick start

Check that a file is valid RDF/XML
```bash
sop parse file.rdf ! Z
```

Convert a JSON-LD file in turtle:
```bash
sop parse file.jsonld ! serialize -o file.ttl
```

Convert a JSON-LD file in turtle and RDF/XML
```bash
sop parse file.jsonld ! serialize -o file.ttl ! serialize -o file.rdf
```

Run a SPARQL query over a file retrieved from the web
```bash
sop parse http://example.org/file.ttl ! query 'SELECT ?t { [] a ?t }'
```

Parse multiple files using internal globbing:
(Useful when the number of files exceeds the shell's argument limit. Note the `m-` terminator)
```bash
sop parse -m "examples/msg-*.nt" m- ! serialize -f nq
```
<details>
<summary>More about globbing</summary>
  
> The internal globbing support uses the [glob](https://crates.io/crates/glob) crate and supports:
> * `?` matches any single character.
> * `*` matches any sequence of characters (except directory separators).
> * `**` matches any sequence of characters including directory separators.
> * `[a-z]` matches any character in the bracketed range.
> * `[!a-z]` matches any character NOT in the bracketed range.

</details>
  
Read Turtle from stdin, remove all language strings that are not in English, and serialize back to Turtle:
```bash
cat examples/lang.ttl | sop parse -f ttl ! filter 'coalesce(langMatches(lang(?o), "en"), true)' ! serialize -f ttl
```
> NB: The `coalesce(..., true)` trick ensures that IRIs and literals without language tags are kept.


Produce the canonical version of a Turtle file, using a fixed base IRI:
```bash
sop parse examples/social.ttl --base x-dummy-base: ! canonicalize -o examples/social.c14n.nq
```

Add a graph name to all triples from an `.nt` file:
```bash
sop parse examples/sample.nt ! map -g "<http://example.org/graph>" ! serialize -f nq
```
> NB: The arguments to `map` are SPARQL expressions; that's why IRIs must be enclosed in in `<...>`.

Map each triple in a named graph named after its subject:
```bash
sop parse examples/sample.nt ! map -g "?s" ! serialize -f nq
```
> NB: you might need to quote variables like `"?s"` to avoid shell expansion


Lower-case all predicate IRIs:
```bash
sop parse examples/social.ttl ! map -p "iri(lcase(str(?p)))" ! serialize -f ttl
```

## JSON-LD Document loader

By default, the JSON-LD processor will only accept inline contexts.
Two [document loaders](https://www.w3.org/TR/json-ld11-api/#remote-document-and-context-retrieval)
are available via command-line options:

* a local document loader (`--loader_local` or `-l`):
  this option expects a path to a directory.
  Every file or subdirectory `ITEM` of that path is interpreted as a local cache for the
  `https://ITEM/` namespace.

* a URL document loader (`--loader_url` or `-u`):
  with this option, any context IRI will be fetched
  (from the Web or from the filesystem, depending).
  This option is provided for convenience,
  but is not fit for production as it presents
  [security](https://www.w3.org/TR/json-ld11/#iana-security)
  and [privacy](https://www.w3.org/TR/json-ld11/#privacy) issues.

With both options, the local version will be used in priority.

## Advanced Commands

### Merge

Merge all named graphs into the default graph.
Use `--drop` to keep ONLY the merged default graph and discard the named graphs.
```bash
sop parse examples/msg-1.nt ! map -g "<http://example.org/g1>" ! merge --drop ! serialize -f nq
```

### Null

Silently consume all quads and only report errors. Useful for validation.
```bash
sop parse examples/social.ttl ! null
```

## Subcommand Aliases

Most subcommands have short aliases for convenience:

* `parse`: `p`
* `serialize`: `s`
* `filter`: `f`
* `map`: `ma`
* `merge`: `me`
* `query`: `q`
* `relativize`: `r`
* `canonicalize`: `c14n`, `c`
* `absolutize`: `a`
* `null`: `n`, `Z`

Example using aliases:
```bash
sop p examples/sample.nt ! f "?p = <http://example.org/p>" ! s -f nt
```

**IMPORTANT**: these aliases are provided for convenience,
but not guaranteed to be stable (in particular, new subcommands may create ambiguity).
You can use them interactively, but in reusable scripts, stick to the full names.

## Supported Formats

`sop` supports a wide range of RDF concrete syntaxes and their most common aliases.
The format is automatically guessed from the file extension or HTTP headers when possible,
but can be overridden using the `--format` (or `-f`) option.

| Format | Common Aliases |
| :--- | :--- |
| **Turtle** | `turtle`, `ttl`, `text/turtle` |
| **JSON-LD** | `jsonld`, `json`, `application/ld+json` |
| **N-Triples** | `nt`, `ntriples`, `application/n-triples` |
| **N-Quads** | `nq`, `nquads`, `application/n-quads` |
| **TriG** | `trig`, `application/trig` |
| **RDF/XML** | `rdf`, `xml`, `application/rdf+xml` |
| **YAML-LD** | `yamlld`, `yml`, `yaml` |
| **Generalized N-Quads** | `gnq`, `gn-quads` |
| **Generalized TriG** | `gtrig`, `text/rdf+n3` |
