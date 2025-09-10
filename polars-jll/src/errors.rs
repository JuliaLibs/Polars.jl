use jlrs::data::managed::ccall_ref::CCallRefRet;
pub use jlrs::prelude::*;

use crate::leak_value;

#[derive(OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_error_t {
  pub msg: String,
}

pub fn make_error<S: ToString>(msg: S) -> CCallRefRet<polars_error_t> {
  leak_value(polars_error_t { msg: msg.to_string() })
}
