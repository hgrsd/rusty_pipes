use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum Operation {
    Filter { predicate: String },
    InnerJoin { on: String },
}

#[derive(Deserialize, Debug, Clone)]
pub struct TransformationDefinition {
    pub sources: Vec<String>,
    pub operations: Vec<Operation>,
}
