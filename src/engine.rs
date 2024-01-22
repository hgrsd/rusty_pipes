use rayon::prelude::*;
use std::{collections::HashMap, path::Path};

use crate::{
    core::{
        context::Context,
        dataframe::Dataframe,
        definitions::{Operation, PipelineDefinition, Source, TransformationDefinition},
        loader::Loader,
        result::RustyPipesResult,
        transformation::Transformation,
    },
    loaders::FileLoader,
    transformations::{Filter, InnerJoin},
};

fn build_pipeline<'a>(
    definition: &'a TransformationDefinition,
    context: &Context,
) -> RustyPipesResult<Vec<Box<dyn Transformation + 'a>>> {
    let mut transformations = vec![];
    for op_def in &definition.operations {
        let op: Box<dyn Transformation> = match op_def {
            Operation::Filter { predicate } => Box::new(Filter::new(predicate, context)?),
            Operation::InnerJoin { on } => Box::new(InnerJoin::new(on)?),
        };
        transformations.push(op);
    }
    Ok(transformations)
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

    fn load_dataframes(&self) -> HashMap<String, RustyPipesResult<Dataframe>> {
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
                (name.clone(), loader.load())
            })
            .collect()
    }

    /// Run the pipeline. This will:
    /// - fetch data from the defined data sources
    /// - run each transformation
    /// - yield a map of each transformation output, keyed by their name
    pub fn run(&mut self, context: &Context) -> HashMap<String, RustyPipesResult<Vec<Dataframe>>> {
        let dfs = self.load_dataframes();

        self.pipeline_definition
            .transformations
            .par_iter()
            .map(|(name, definition)| {
                let name = name.clone();

                let pipeline = build_pipeline(definition, context);
                if let Err(err) = pipeline {
                    return (name, Err(err));
                }
                let unwrapped_pipeline = pipeline.unwrap();
                let mut pipeline_iter = unwrapped_pipeline.iter();

                let source_dataframes: RustyPipesResult<Vec<&Dataframe>> = definition
                    .sources
                    .iter()
                    .map(|source| dfs.get(source).unwrap().as_ref().map_err(|err| err.clone()))
                    .collect();

                let mut current_output = if let Some(first_transformation) = pipeline_iter.next() {
                    source_dataframes.and_then(|x| first_transformation.transform(&x))
                } else {
                    Ok(vec![])
                };

                for transformation in pipeline_iter {
                    current_output = current_output.and_then(|output| {
                        let refs = output.iter().collect();
                        transformation.transform(&refs)
                    });
                }

                (name, current_output)
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::core::dataframe::ColumnValue;

    use super::*;

    #[test]
    fn it_runs_a_pipeline() {
        let raw_definition = std::fs::read_to_string("examples/filter.json").unwrap();
        let parsed: PipelineDefinition = serde_json::from_str(&raw_definition).unwrap();
        let mut engine = Engine::from_definition(parsed);
        let result = engine.run(&Default::default());
        assert_eq!(result.len(), 1);

        let filtered = result.get("filtered").unwrap();
        assert_eq!(
            filtered.as_ref().unwrap()[0],
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
