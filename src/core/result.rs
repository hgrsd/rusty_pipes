use super::error::RustyPipesError;

pub type RustyPipesResult<T> = std::result::Result<T, RustyPipesError>;
