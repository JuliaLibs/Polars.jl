use jlrs::{data::managed::string::StringRet, prelude::*};

use crate::utils::leak_string;

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_type_t {
  pub inner: polars::prelude::DataType,
}

impl polars_value_type_t {
  pub fn display(&self) -> StringRet {
    leak_string(format!("{}", self.inner))
  }
}
