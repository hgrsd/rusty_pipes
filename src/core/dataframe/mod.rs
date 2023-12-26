use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub enum ColumnValue {
    Integer(i64),
    Decimal(f64),
    String(String),
}

pub type Row = HashMap<String, ColumnValue>;

pub type Dataframe = Vec<Row>;
