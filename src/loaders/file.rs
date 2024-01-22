use std::{collections::HashMap, path::Path};

use crate::core::{
    dataframe::{ColumnValue, Dataframe, Row},
    definitions::{ColumnDefinition, DataType, Format},
    error::RustyPipesError,
    loader::Loader,
    result::RustyPipesResult,
};

pub struct FileLoader<'a> {
    path: &'a Path,
    format: &'a Format,
    schema: &'a Vec<ColumnDefinition>,
}

impl<'a> FileLoader<'a> {
    /// Construct a new file loader for the given path, format, and using the specified schema. This is a lazy
    /// operation; until the "load" method is run, no work will be performed.
    pub fn new(path: &'a Path, format: &'a Format, schema: &'a Vec<ColumnDefinition>) -> Self {
        FileLoader {
            path,
            format,
            schema,
        }
    }

    fn load_csv(&self) -> RustyPipesResult<Dataframe> {
        let mut reader = csv::Reader::from_path(self.path)
            .map_err(|e| RustyPipesError::LoaderError(e.to_string()))?;
        let mut df = vec![];
        for row_raw in reader.records() {
            let mut row: Row = HashMap::new();
            let result = row_raw.map_err(|e| RustyPipesError::LoaderError(e.to_string()))?;
            for (i, definition) in self.schema.iter().enumerate() {
                let value = &result[i];
                let parsed_value = match definition.data_type {
                    DataType::Integer => ColumnValue::Integer(
                        value
                            .parse::<i64>()
                            .map_err(|e| RustyPipesError::LoaderError(e.to_string()))?,
                    ),
                    DataType::Decimal => ColumnValue::Decimal(
                        value
                            .parse::<f64>()
                            .map_err(|e| RustyPipesError::LoaderError(e.to_string()))?,
                    ),
                    DataType::String => ColumnValue::String(value.to_owned()),
                };
                row.insert(definition.column_name.clone(), parsed_value);
            }
            df.push(row);
        }
        Ok(df)
    }
}

impl Loader for FileLoader<'_> {
    fn load(&self) -> RustyPipesResult<Dataframe> {
        match self.format {
            Format::Csv => self.load_csv(),
        }
    }
}
