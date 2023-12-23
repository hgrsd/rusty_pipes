use crate::definitions::pipeline::PipelineDefinition;

mod definitions;

fn main() {
    let file = std::fs::read_to_string("./examples/pipeline.json").unwrap();
    let parsed: PipelineDefinition = serde_json::from_str(&file).unwrap();
    println!("{:?}", parsed);
}
