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
