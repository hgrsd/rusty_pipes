use std::{error::Error, fmt::Display};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub enum RustyPipesError {
    LoaderError(String),
    TransformationError(String),
}

impl Display for RustyPipesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustyPipesError::LoaderError(s) => write!(f, "{}", s),
            RustyPipesError::TransformationError(s) => write!(f, "{}", s),
        }
    }
}

impl Error for RustyPipesError {}
