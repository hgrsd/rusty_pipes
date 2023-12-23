use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub enum DataType {
    INTEGER,
    DECIMAL,
    STRING,
}

#[derive(Deserialize, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub mandatory: bool,
}
