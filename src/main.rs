use std::{error::Error};

use crate::engine::Engine;
use crate::{
    definitions::{PipelineDefinition},
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

    let output = engine.run()?;

    println!("{:?}", output);
    Ok(())
}
