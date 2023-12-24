use std::{collections::HashMap, error::Error, path::Path};

use crate::definitions::TransformationDefinition;
use crate::{
    dataframe::DataFrame,
    definitions::{Operation, PipelineDefinition, Source},
    loaders::{FileLoader, Loader},
    transformations::{Filter, Transformation},
};

pub struct Engine {
    pipeline_definition: PipelineDefinition,
    loaded_dataframes: HashMap<String, DataFrame>,
}

impl Engine {
    pub fn from_definition(pipeline_definition: PipelineDefinition) -> Self {
        Engine {
            pipeline_definition,
            loaded_dataframes: HashMap::new(),
        }
    }

    fn load_dataframes(
        &mut self,
        source_names: &[String],
    ) -> Result<Vec<&DataFrame>, Box<dyn Error>> {
        for name in source_names.iter() {
            if self.loaded_dataframes.contains_key(name) {
                continue;
            }
            let definition = self.pipeline_definition.sources.get(name).unwrap();
            let loader = match &definition.source {
                Source::FILE { path, format } => {
                    let path = Path::new(path);
                    FileLoader::new(&path, format, &definition.schema)
                }
            };
            let loaded_dataframe = loader.load()?;
            self.loaded_dataframes
                .insert(name.to_owned(), loaded_dataframe);
        }

        let mut result = vec![];
        for name in source_names.iter() {
            result.push(self.loaded_dataframes.get(name).unwrap());
        }
        Ok(result)
    }

    fn build_pipeline(
        &self,
        definition: &TransformationDefinition,
    ) -> Vec<Box<dyn Transformation>> {
        let mut transformations = vec![];
        for operation in &definition.operations {
            transformations.push(match operation {
                Operation::FILTER { predicate } => {
                    Box::new(Filter::new(&predicate)) as Box<dyn Transformation>
                }
            })
        }
        transformations
    }

    pub fn run(&mut self) -> Result<HashMap<String, DataFrame>, Box<dyn Error>> {
        let mut result = HashMap::new();
        let transformations: HashMap<String, TransformationDefinition> =
            self.pipeline_definition.transformations.clone();

        for (name, definition) in transformations {
            let pipeline = self.build_pipeline(&definition);
            let data_frames = self.load_dataframes(&definition.sources)?;
            let transformed = pipeline
                .into_iter()
                .fold(data_frames[0].clone(), |acc, transformer| {
                    transformer.transform(acc)
                });
            result.insert(name.to_owned(), transformed);
        }

        Ok(result)
    }
}
