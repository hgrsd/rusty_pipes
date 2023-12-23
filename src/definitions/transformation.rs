use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Operation {
    FILTER { predicate: String }
}

#[derive(Deserialize, Debug)]
pub struct TransformationDefinition {
    pub sources: Vec<String>,
    pub operations: Vec<Operation>,
}
