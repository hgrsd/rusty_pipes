use serde::Deserialize;

use super::column::ColumnDefinition;

#[derive(Deserialize, Debug)]
pub enum Format {
    CSV,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Location {
    FILE { path: String },
}

#[derive(Deserialize, Debug)]
pub struct DataSourceDefinition {
    pub schema: Vec<ColumnDefinition>,
    pub format: Format,
    pub location: Location,
}
