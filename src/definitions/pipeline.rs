use std::collections::HashMap;

use serde::Deserialize;

use super::{data_source::DataSourceDefinition, transformation::TransformationDefinition};

#[derive(Deserialize, Debug)]
pub struct PipelineDefinition {
    pub sources: HashMap<String, DataSourceDefinition>,
    pub transformations: HashMap<String, TransformationDefinition>,
}
