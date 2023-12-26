use serde::Serialize;
use std::collections::HashMap;

/// Wrapper type for a column value.
#[derive(Debug, Serialize, Clone, PartialEq)]
pub enum ColumnValue {
    Integer(i64),
    Decimal(f64),
    String(String),
}

/// A type alias for a row, which is implemented as a hash map from column name to value.
pub type Row = HashMap<String, ColumnValue>;

/// A type alias for the core data type on which this crate operates. A data frame is a vector of rows.
pub type Dataframe = Vec<Row>;
