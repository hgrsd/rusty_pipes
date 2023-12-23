use std::{collections::HashMap, error::Error, path::Path};

use dataframe::file::FileLoader;

use crate::{
    dataframe::{loader::Loader, DataFrame},
    definitions::{Location, Operation, PipelineDefinition},
    transformations::{Filter, Transformation},
};

mod dataframe;
mod definitions;
mod transformations;

fn main() -> Result<(), Box<dyn Error>> {
    let file = std::fs::read_to_string("./examples/pipeline.json").unwrap();
    let parsed: PipelineDefinition = serde_json::from_str(&file).unwrap();
    let s0 = parsed.sources.get("s0").unwrap();

    let df = match &s0.location {
        Location::FILE { path } => {
            let path = Path::new(&path);
            let loader = FileLoader::new(&path, &s0.format, &s0.schema);
            loader.load()?
        }
    };

    let transformed: HashMap<String, DataFrame> = parsed
        .transformations
        .iter()
        .map(|(key, definition)| {
            let transformed = definition.operations.iter().fold(df.clone(), |acc, cur| {
                let op = match cur {
                    Operation::FILTER { predicate } => Filter::new(&predicate),
                };
                op.transform(&acc)
            });
            (key.clone(), transformed)
        })
        .collect();

    println!("{:?}", transformed);
    Ok(())
}
