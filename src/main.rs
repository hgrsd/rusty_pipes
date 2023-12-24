use std::{collections::HashMap, error::Error, path::Path};

use crate::engine::Engine;
use crate::{
    dataframe::DataFrame,
    definitions::{Operation, PipelineDefinition, Source},
    loaders::{FileLoader, Loader},
    transformations::{Filter, Transformation},
};

mod dataframe;
mod definitions;
mod engine;
mod loaders;
mod transformations;

fn main() -> Result<(), Box<dyn Error>> {
    let file = std::fs::read_to_string("./examples/pipeline.json").unwrap();
    let pipeline: PipelineDefinition = serde_json::from_str(&file).unwrap();
    let mut engine = Engine::from_definition(pipeline);

    let output = engine.run();

    println!("{:?}", output);
    Ok(())
}
