use anyhow::bail;
use sophia::{
    api::prefix::{Prefix, PrefixMapPair},
    iri::Iri,
};

/// Parse a prefix map expressed as PREFIX:URI,PREFIX:URI,...
pub fn parse_prefix_map(txt: &str) -> Result<Vec<PrefixMapPair>, anyhow::Error> {
    txt.split(",")
        .map(|item| {
            let pair = item.splitn(2, ":").collect::<Vec<_>>();
            if pair.len() != 2 {
                bail!("Missing colon (':') in prefix-map entry {item:?}");
            };
            let prefix = Prefix::new(Box::from(pair[0]))?;
            let iri = Iri::new(Box::from(pair[1]))?;
            Ok((prefix, iri))
        })
        .collect()
}
