use serde::Deserialize;

use super::column::ColumnDefinition;

#[derive(Deserialize, Debug)]
pub enum Format {
    CSV,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Source {
    FILE { path: String, format: Format },
}

#[derive(Deserialize, Debug)]
pub struct DataSourceDefinition {
    pub schema: Vec<ColumnDefinition>,
    pub source: Source,
}
