use std::error::Error;

use rusty_pipes::{core::definitions::PipelineDefinition, engine::Engine};

fn main() -> Result<(), Box<dyn Error>> {
    let raw_filter_definition = std::fs::read_to_string("examples/filter.json")?;
    let parsed_filter_definition: PipelineDefinition =
        serde_json::from_str(&raw_filter_definition)?;
    let mut filter_engine = Engine::from_definition(parsed_filter_definition);
    let filter_result = filter_engine.run(&Default::default());

    let raw_join_definition = std::fs::read_to_string("examples/join.json")?;
    let parsed_join_definition: PipelineDefinition = serde_json::from_str(&raw_join_definition)?;
    let mut join_engine = Engine::from_definition(parsed_join_definition);
    let join_result = join_engine.run(&Default::default());

    println!(
        "filter: \n------\n{}\n------",
        serde_json::to_string_pretty(&filter_result)?
    );
    println!(
        "join: \n------\n{}\n------",
        serde_json::to_string_pretty(&join_result)?
    );

    Ok(())
}
