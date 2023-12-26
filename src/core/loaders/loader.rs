use std::error::Error;

use crate::core::dataframe::Dataframe;

/// A Loader is a struct that can yield a data frame. Individual loaders are expected to implement this trait.
pub trait Loader {
    fn load(&self) -> Result<Dataframe, Box<dyn Error>>;
}
