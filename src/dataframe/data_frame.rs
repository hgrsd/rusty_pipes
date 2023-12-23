use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub enum ColumnValue {
    Integer(i64),
    Decimal(f64),
    String(String),
}

pub type DataFrame = Vec<HashMap<String, ColumnValue>>;
