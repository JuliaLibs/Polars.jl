use jlrs::data::managed::{ccall_ref::CCallRefRet, string::StringRet};
pub use jlrs::prelude::*;

use crate::utils::{leak_string, leak_value};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_error_t {
  pub msg: String,
}

impl polars_error_t {
  pub fn new(msg: JuliaString) -> CCallRefRet<Self> {
    leak_value(Self { msg: unsafe { msg.as_str_unchecked() }.to_string() })
  }

  pub fn message(&self) -> StringRet {
    leak_string(&self.msg)
  }
}

pub fn make_error<S: ToString>(msg: S) -> CCallRefRet<polars_error_t> {
  leak_value(polars_error_t { msg: msg.to_string() })
}
