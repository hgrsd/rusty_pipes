use serde::Deserialize;

use super::column::ColumnDefinition;

/// The format of a data source.
#[derive(Deserialize, Debug)]
pub enum Format {
    Csv,
}

/// The definition of the data source, defining where/how to source the data.
#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Source {
    File { path: String, format: Format },
}

/// A definition for a data source.
#[derive(Deserialize, Debug)]
pub struct DataSourceDefinition {
    /// The schema of this data source, expressed as a list of columns definitions.
    pub schema: Vec<ColumnDefinition>,
    /// The source definition.
    pub source: Source,
}
