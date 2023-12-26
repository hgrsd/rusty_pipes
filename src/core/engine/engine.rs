use rayon::prelude::*;
use std::{collections::HashMap, path::Path};

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

/// The engine is the entry point for running a pipeline. It is constructed based on a pipeline definition.
/// It then sources the data and runs the transformations, yielding the outputs of each transformation.
pub struct Engine {
    pipeline_definition: PipelineDefinition,
}

impl Engine {
    /// Construct an Engine based on a given pipeline definition.
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

    /// Run the pipeline. This will:
    /// - fetch data from the defined data sources
    /// - run each transformation
    /// - yield a map of each transformation output, keyed by their name
    pub fn run(&mut self) -> HashMap<String, Vec<Dataframe>> {
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

        result
    }
}
