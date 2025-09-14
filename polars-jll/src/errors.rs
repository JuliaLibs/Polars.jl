use std::fmt::Debug;

use jlrs::{data::managed::{string::StringRet, value::ValueRet}, error::JlrsError, runtime::handle::ccall::throw_exception, weak_handle};
pub use jlrs::prelude::*;
use polars::error::PolarsError;

use crate::utils::leak_string;

#[derive(Debug, thiserror::Error)]
pub enum JuliaPolarsError {
  #[error("Julia error: {0}")]
  JlrsError(Box<JlrsError>),
  #[error("Polars error: {0}")]
  PolarsError(#[from] PolarsError),
  #[error("Io error: {0}")]
  IoError(#[from] std::io::Error),
  #[error("{0} called without a valid Julia context")]
  WeakHandleError(&'static str),
  #[error("Error calling Julia function {0}")]
  CallFunctionError(&'static str, Option<String>),
  #[error("Error extracting value at index {0}")]
  ExtractBoxError(usize),
  #[error("Missing field {0} in NamedTuple")]
  NamedTupleMissingField(String),
  #[error("Unknown time unit: {0}")]
  TimeUnitError(String),
  #[error("Unsupported data type: {0}")]
  UnsupportedDataType(String),
  #[error("Unsupported AnyValue variant: {0}")]
  UnsupportedAnyValue(&'static str),
}

impl From<JuliaPolarsError> for Box<JlrsError> {
  fn from(err: JuliaPolarsError) -> Self {
    match err {
      JuliaPolarsError::JlrsError(e) => e,
      JuliaPolarsError::PolarsError(e) => Box::new(JlrsError::other(e)),
      _ => Box::new(JlrsError::exception(err.to_string())),
    }
  }
}

impl JuliaPolarsError {
  pub fn panic(self) -> ! {
    panic!("{}", self.to_string())
  }

  pub fn function_call<E: Debug>(func: &'static str, e: E) -> Self {
    JuliaPolarsError::CallFunctionError(func, Some(format!("{e:?}")))
  }
}

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_error_t {
  pub(crate) inner: JuliaPolarsError,
}

impl polars_error_t {
  pub fn message(&self) -> StringRet {
    let msg = format!("{}", self.inner);
    leak_string(msg)
  }

  pub fn throw(self) -> ! {
    let exception = self.into();
    unsafe { throw_exception(exception) }
  }
}

impl From<polars_error_t> for ValueRet {
  fn from(err: polars_error_t) -> Self {
    match weak_handle!() {
      Ok(handle) => {
        let exception = Value::new(&handle, err);
        exception.leak()
      },
      Err(_) => JuliaPolarsError::WeakHandleError("ValueRet::from::<polars_error_t>").panic(),
    }
  }
}
