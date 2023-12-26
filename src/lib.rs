/// The core of rusty_pipes, which contains a means of defining a pipeline as well as the engine which can perform
/// the operations defined in such a pipeline.
pub mod core;

pub use core::dataframe;
pub use core::definitions;
pub use core::engine;
pub use core::loaders;
pub use core::transformations;
