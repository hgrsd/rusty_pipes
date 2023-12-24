use std::{collections::HashMap, error::Error, path::Path};

use crate::{
    dataframe::{ColumnValue, DataFrame},
    definitions::{ColumnDefinition, DataType, Format},
};

use super::loader::Loader;

pub struct FileLoader<'a> {
    path: &'a Path,
    format: &'a Format,
    schema: &'a Vec<ColumnDefinition>,
}

impl<'a> FileLoader<'a> {
    pub fn new(path: &'a Path, format: &'a Format, schema: &'a Vec<ColumnDefinition>) -> Self {
        FileLoader {
            path,
            format,
            schema,
        }
    }

    fn load_csv(&self) -> Result<DataFrame, Box<dyn Error>> {
        let mut reader = csv::Reader::from_path(self.path)?;
        let mut df = vec![];
        for row in reader.records() {
            let mut cols: HashMap<String, ColumnValue> = HashMap::new();
            let result = row?;
            for (i, definition) in self.schema.iter().enumerate() {
                let value = &result[i];
                let parsed_value = match definition.data_type {
                    DataType::Integer => ColumnValue::Integer(value.parse::<i64>()?),
                    DataType::Decimal => ColumnValue::Decimal(value.parse::<f64>()?),
                    DataType::String => ColumnValue::String(value.to_owned()),
                };
                cols.insert(definition.column_name.clone(), parsed_value);
            }
            df.push(cols);
        }
        Ok(df)
    }
}

impl Loader for FileLoader<'_> {
    fn load(&self) -> Result<DataFrame, Box<dyn Error>> {
        match self.format {
            Format::CSV => self.load_csv(),
        }
    }
}
