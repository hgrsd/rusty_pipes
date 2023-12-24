use std::{collections::HashMap, error::Error, path::Path};

use crate::definitions::TransformationDefinition;
use crate::{
    dataframe::DataFrame,
    definitions::{Operation, PipelineDefinition, Source},
    loaders::{FileLoader, Loader},
    transformations::{Filter, Transformation},
};
use crate::transformations::InnerJoin;

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

    fn load(
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
        definition: &TransformationDefinition,
    ) -> Vec<Box<dyn Transformation>> {
        let mut transformations = vec![];
        for operation in &definition.operations {
            transformations.push(match operation {
                Operation::Filter { predicate } => {
                    Box::new(Filter::new(predicate)) as Box<dyn Transformation>
                },
                Operation::InnerJoin { on } => {
                    Box::new(InnerJoin::new(on)) as Box<dyn Transformation>
                }
            })
        }
        transformations
    }

    pub fn run(&mut self) -> Result<HashMap<String, Vec<DataFrame>>, Box<dyn Error>> {
        let mut result = HashMap::new();

        for (name, definition) in self.pipeline_definition.transformations.clone() {
            let pipeline = Engine::build_pipeline(&definition);
            let data_frames = self.load(&definition.sources)?.into_iter().cloned().collect();
            let transformed = pipeline
                .into_iter()
                .fold(data_frames, |acc, transformer| {
                    transformer.transform(acc)
                });
            result.insert(name.to_owned(), transformed);
        }

        Ok(result)
    }
}
