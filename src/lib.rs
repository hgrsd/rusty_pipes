/// The core of rusty_pipes, which contains a means of defining a pipeline as well as the engine which can perform
/// the operations defined in such a pipeline.
pub mod core;

/// Engine implementation that can drive a transformation pipeline
pub mod engine;

/// Loader implementations
pub mod loaders;

/// Transformation implementations
pub mod transformations;
