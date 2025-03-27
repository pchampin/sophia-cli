Semantic Operation Pipeline
===========================

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


## Quick start

Check that a file is valid RDF/XML
```
sop parse file.rdf ! Z
```

Convert an JSON-LD file in turtle:
```
sop parse file.jsonld ! serialize -o file.ttl
```

Run a SPARQL query over a file retrieved from the web
(**caveat**: SPARQL support is *very* partial at the moment)
```
sop parse http://example.org/file.ttl ! query 'SELECT ?t { [] a ?t }'
```

Read Turtle from stdin, remove all language strings that are not in english, and serialize back to turtle
```
sop parse -f ttl ! filter 'coalesce(langMatches(lang(?o), "en"), true)' ! serialize -f ttl
```

Produce the canonical version of a turtle file, using a fixed base IRI
```
sop parse file.ttl --base x-dummy-base: ! canonicalize -o file.c14n.nq
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
