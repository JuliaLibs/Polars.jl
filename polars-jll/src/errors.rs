use jlrs::{data::managed::{string::StringRet, value::ValueRet}, error::JlrsError, runtime::handle::ccall::throw_exception, weak_handle};
pub use jlrs::prelude::*;
use polars::error::PolarsError;

use crate::utils::leak_string;

#[derive(Debug, thiserror::Error)]
pub enum JuliaPolarsError {
  #[error("Julia error: {0}")]
  JlrsError(Box<JlrsError>),
  #[error("Polars error: {0}")]
  PolarsError(PolarsError),
  #[error("{0} called without a valid Julia context")]
  WeakHandleError(&'static str),
}

impl JuliaPolarsError {
  pub fn panic(self) -> ! {
    panic!("{}", self.to_string())
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
