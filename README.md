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

## Quick start

```
./sop --help
```

TODO show how to do basic tasks with sop...

## JSON-LD Document loader

By default, the JSON-LD processor will only accept inline contexts.
Two [document loaders](https://www.w3.org/TR/json-ld11-api/#remote-document-and-context-retrieval)
are available via command-line options:

* a local document loader (`--loader_local` or `-l`):
  this option expects a path to a directory.
  Every subdirectory `SUBDIR` of that path is interpreted as a local cache for the
  `https://SUBDIR/` namespace.

* a URL document loader (`--loader_url` or `-u`):
  with this option, any HTTP(S) context IRI will be fetched from the Web.
  This option is provided for convenience,
  but is not fit for production as it presents
  [security](https://www.w3.org/TR/json-ld11/#iana-security)
  and [privacy](https://www.w3.org/TR/json-ld11/#privacy) issues.

With both options, the local version will be used in priority.
