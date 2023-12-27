use rayon::prelude::*;
use std::{collections::HashMap, path::Path};

use crate::core::context::Context;
use crate::definitions::TransformationDefinition;
use crate::{
    dataframe::Dataframe,
    definitions::{Operation, PipelineDefinition, Source},
    loaders::{FileLoader, Loader},
    transformations::{Filter, InnerJoin, Transformation},
};

fn build_pipeline<'a>(
    definition: &'a TransformationDefinition,
    context: &'a Context,
) -> impl Iterator<Item = Box<dyn Transformation>> + 'a {
    definition.operations.iter().map(|def| {
        let op: Box<dyn Transformation> = match def {
            Operation::Filter { predicate } => Box::new(Filter::new(predicate, context)),
            Operation::InnerJoin { on } => Box::new(InnerJoin::new(on)),
        };
        op
    })
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

    fn load_dataframes(&self) -> HashMap<String, Dataframe> {
        self.pipeline_definition
            .sources
            .par_iter()
            .map(|(name, definition)| {
                let loader = match &definition.source {
                    Source::File { path, format } => {
                        let path = Path::new(path);
                        FileLoader::new(path, format, &definition.schema)
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
    pub fn run(&mut self, context: &Context) -> HashMap<String, Vec<Dataframe>> {
        let dfs = self.load_dataframes();

        let result = self
            .pipeline_definition
            .transformations
            .par_iter()
            .map(|(name, definition)| {
                let pipeline = build_pipeline(definition, context);
                let source_dataframes = definition
                    .sources
                    .iter()
                    .map(|source| dfs.get(source).unwrap())
                    .collect();

                let mut result: Option<Vec<Dataframe>> = None;
                for transformation in pipeline {
                    match result {
                        None => {
                            // if we are in this arm, we are doing the first transformation in the pipeline; so we take
                            // the source dataframes as our input
                            result = Some(transformation.transform(&source_dataframes));
                        }
                        Some(previous_result) => {
                            // otherwise, we apply the current transformation to the previous result
                            let refs = previous_result.iter().collect();
                            result = Some(transformation.transform(&refs));
                        }
                    }
                }
                (name.clone(), result.unwrap_or_default())
            })
            .collect();

        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataframe::ColumnValue;

    #[test]
    fn it_runs_a_pipeline() {
        let raw_definition = std::fs::read_to_string("examples/filter.json").unwrap();
        let parsed: PipelineDefinition = serde_json::from_str(&raw_definition).unwrap();
        let mut engine = Engine::from_definition(parsed);
        let result = engine.run(&Default::default());

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.get("filtered").unwrap()[0],
            vec![HashMap::from([
                (
                    String::from("first_name"),
                    ColumnValue::String(String::from("Jen"))
                ),
                (String::from("salary"), ColumnValue::Decimal(19319.0)),
            ])]
        );
    }
}
