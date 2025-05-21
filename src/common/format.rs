use std::sync::LazyLock;

use anyhow::Error;
use regex::{RegexSet, RegexSetBuilder};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Format {
    GeneralizedNQuads,
    GeneralizedTriG,
    JsonLd,
    Hdt,
    NQuads,
    NTriples,
    RdfXml,
    TriG,
    Turtle,
    YamlLd,
}

pub use Format::*;

impl std::str::FromStr for Format {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static RES: LazyLock<RegexSet> = LazyLock::new(|| {
            RegexSetBuilder::new([
                r"^( generalized-n-?quads | gn-?quads | gnq )$",
                r"^( generalized-trig | gtrig | text/rdf\+n3 )$",
                r"^( application/ld\+json | json-?ld | application/json | json )$",
                r"^( application/ld\+yaml| yaml-?ld | ymlld | application/yaml | yaml | yml )$",
                r"^( application/n-quads | n-?quads | nq )",
                r"^( application/n-triples | n-?triples | nt | text/plain )",
                r"^( application/rdf\+xml | rdf | rdf/?xml | application/xml | xml )$",
                r"^( application/trig | trig )",
                r"^( application/vnd.hdt | hdt )",
                r"^( text/turtle | turtle | ttl | application/turtle )",
            ])
            .ignore_whitespace(true)
            .case_insensitive(true)
            .build()
            .unwrap()
        });
        match RES.matches(s).iter().next() {
            Some(0) => Ok(GeneralizedNQuads),
            Some(1) => Ok(GeneralizedTriG),
            Some(2) => Ok(JsonLd),
            Some(3) => Ok(YamlLd),
            Some(4) => Ok(NQuads),
            Some(5) => Ok(NTriples),
            Some(6) => Ok(RdfXml),
            Some(7) => Ok(TriG),
            Some(8) => Ok(Hdt),
            Some(9) => Ok(Format::Turtle),
            _ => Err(Error::msg(format!("Unrecognized format: {s}"))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    #[test_case("generalized-n-quads" => GeneralizedNQuads)]
    #[test_case("generalized-nquads" => GeneralizedNQuads)]
    #[test_case("gn-quads" => GeneralizedNQuads)]
    #[test_case("gnquads" => GeneralizedNQuads)]
    #[test_case("GNQuads" => GeneralizedNQuads; "gnquads cam")]
    #[test_case("gnq" => GeneralizedNQuads)]
    #[test_case("GNQ" => GeneralizedNQuads; "gnq cap")]
    #[test_case("generalized-trig" => GeneralizedTriG)]
    #[test_case("gtrig" => GeneralizedTriG)]
    #[test_case("GTriG" => GeneralizedTriG; "gtrig cam")]
    #[test_case("GTRIG" => GeneralizedTriG; "gtrig cap")]
    #[test_case("text/rdf+n3" => GeneralizedTriG)] // used by DBPedia
    #[test_case("application/ld+json" => JsonLd)]
    #[test_case("json-ld" => JsonLd)]
    #[test_case("JSON-LD" => JsonLd; "json-ld cap")]
    #[test_case("jsonld" => JsonLd)]
    #[test_case("JsonLd" => JsonLd; "jsonld cam")]
    #[test_case("JSONLD" => JsonLd; "jsonld cap")]
    #[test_case("application/json" => JsonLd)]
    #[test_case("json" => JsonLd)]
    #[test_case("JSON" => JsonLd; "json cap")]
    #[test_case("application/vnd.hdt" => Hdt)]
    #[test_case("hdt" => Hdt)]
    #[test_case("application/n-quads" => NQuads)]
    #[test_case("n-quads" => NQuads)]
    #[test_case("N-Quads" => NQuads; "n-quads cam")]
    #[test_case("nquads" => NQuads)]
    #[test_case("NQuads" => NQuads; "nquads cam")]
    #[test_case("nq" => NQuads)]
    #[test_case("NQ" => NQuads; "nq cap")]
    #[test_case("application/n-triples" => NTriples)]
    #[test_case("n-triples" => NTriples)]
    #[test_case("N-Triples" => NTriples; "n-triples cam")]
    #[test_case("ntriples" => NTriples)]
    #[test_case("NTriples" => NTriples; "ntriples cam")]
    #[test_case("nt" => NTriples)]
    #[test_case("NT" => NTriples; "nt cap")]
    #[test_case("text/plain" => NTriples)]
    #[test_case("application/rdf+xml" => RdfXml)]
    #[test_case("rdf" => RdfXml)]
    #[test_case("RDF" => RdfXml; "rdf cap")]
    #[test_case("RDF/XML" => RdfXml)]
    #[test_case("rdfxml" => RdfXml)]
    #[test_case("application/xml" => RdfXml)]
    #[test_case("xml" => RdfXml)]
    #[test_case("XML" => RdfXml; "xml cap")]
    #[test_case("application/trig" => TriG)]
    #[test_case("trig" => TriG)]
    #[test_case("TriG" => TriG; "trig cam")]
    #[test_case("TRIG" => TriG; "trig cap")]
    #[test_case("text/turtle" => Turtle)]
    #[test_case("turtle" => Turtle)]
    #[test_case("Turtle" => Turtle; "turtle cam")]
    #[test_case("ttl" => Turtle)]
    #[test_case("TTL" => Turtle; "ttl cap")]
    #[test_case("application/turtle" => Turtle)]
    #[test_case("application/ld+yaml" => YamlLd)]
    #[test_case("yaml-ld" => YamlLd)]
    #[test_case("YAML-LD" => YamlLd; "yaml-ld cap")]
    #[test_case("yamlld" => YamlLd)]
    #[test_case("YamlLd" => YamlLd; "yamlld cam")]
    #[test_case("YAMLLD" => YamlLd; "yamlld cap")]
    #[test_case("ymlld" => YamlLd)]
    #[test_case("YMLLD" => YamlLd; "ymlld cap")]
    #[test_case("application/yaml" => YamlLd)]
    #[test_case("yaml" => YamlLd)]
    #[test_case("YAML" => YamlLd; "yaml cap")]
    #[test_case("yml" => YamlLd)]
    #[test_case("YML" => YamlLd; "yml cap")]
    fn parse_format(txt: &str) -> Format {
        txt.parse().unwrap()
    }
}
