use thiserror::Error;
use quant_common::data::types::DataError;
use crate::exchange::errors::ExchangeError;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Exchange error: {0}")]
    Exchange(#[from] ExchangeError),

    #[error("Data error: {0}")]
    Data(#[from] DataError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Task error: {0}")]
    Task(String),
}