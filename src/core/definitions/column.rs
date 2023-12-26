use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub enum DataType {
    Integer,
    Decimal,
    String,
}

#[derive(Deserialize, Debug)]
pub struct ColumnDefinition {
    pub column_name: String,
    pub data_type: DataType,
    pub mandatory: bool,
}
