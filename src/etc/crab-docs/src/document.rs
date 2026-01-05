use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Document {
    #[serde(alias = "key")]
    pub name: String,
    #[serde(alias = "value")]
    #[serde(default)]
    pub content: Option<String>,
    pub revision: u64,
}

impl Document {}
