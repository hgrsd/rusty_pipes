use std::collections::HashMap;

/// A context that can be accessed by the pipeline at runtime. This can contain things like parameters to be passed to
/// Loaders or Transformations.
pub struct Context {
    parameters: HashMap<String, String>,
}

impl Context {
    /// Construct a new context with the given parameters.
    pub fn new(parameters: HashMap<String, String>) -> Self {
        Context { parameters }
    }

    /// Retrieve the value of a parameter.
    pub fn parameter_value(&self, key: &str) -> Option<&str> {
        self.parameters.get(key).map(|value| value.as_str())
    }
}
