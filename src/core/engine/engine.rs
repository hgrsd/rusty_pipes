use rayon::prelude::*;
use std::{collections::HashMap, error::Error, path::Path};

use crate::definitions::TransformationDefinition;
use crate::{
    dataframe::Dataframe,
    definitions::{Operation, PipelineDefinition, Source},
    loaders::{FileLoader, Loader},
    transformations::{Filter, InnerJoin, Transformation},
};

fn build_pipeline(definition: &TransformationDefinition) -> Vec<Box<dyn Transformation>> {
    definition
        .operations
        .iter()
        .map(|def| {
            let op: Box<dyn Transformation> = match def {
                Operation::Filter { predicate } => Box::new(Filter::new(predicate)),
                Operation::InnerJoin { on } => Box::new(InnerJoin::new(on)),
            };
            op
        })
        .collect()
}

pub struct Engine {
    pipeline_definition: PipelineDefinition,
}

impl Engine {
    pub fn from_definition(pipeline_definition: PipelineDefinition) -> Self {
        Engine {
            pipeline_definition,
        }
    }

    fn load_dataframes(&mut self) -> HashMap<String, Dataframe> {
        self.pipeline_definition
            .sources
            .par_iter()
            .map(|(name, definition)| {
                let loader = match &definition.source {
                    Source::File { path, format } => {
                        let path = Path::new(path);
                        FileLoader::new(&path, format, &definition.schema)
                    }
                };
                (name.clone(), loader.load().unwrap())
            })
            .collect()
    }

    pub fn run(&mut self) -> Result<HashMap<String, Vec<Dataframe>>, Box<dyn Error>> {
        let dfs = self.load_dataframes();

        let result = self
            .pipeline_definition
            .transformations
            .par_iter()
            .map(|(name, definition)| {
                let pipeline = build_pipeline(definition);
                let data_frames = definition
                    .sources
                    .iter()
                    .map(|source| dfs.get(source).unwrap().clone())
                    .collect();
                (
                    name.clone(),
                    pipeline
                        .into_iter()
                        .fold(data_frames, |acc, transformer| transformer.transform(acc)),
                )
            })
            .collect();

        Ok(result)
    }
}
