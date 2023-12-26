use std::collections::HashMap;

use serde::Deserialize;

use super::{data_source::DataSourceDefinition, transformation::TransformationDefinition};

/// The definition for the full pipeline. This contains definitions for the available sources, as well as definitions
/// for the operations to be performed.
#[derive(Deserialize, Debug)]
pub struct PipelineDefinition {
    /// Definitions for the available sources, keyed by their identifier.
    pub sources: HashMap<String, DataSourceDefinition>,
    /// Definitions for the transformation outputs, keyed by their identifier.
    pub transformations: HashMap<String, TransformationDefinition>,
}
