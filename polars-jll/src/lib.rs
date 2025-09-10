use core::panic;

use jlrs::{
  convert::into_julia::IntoJulia,
  data::{
    managed::{ccall_ref::CCallRefRet, string::StringRet, value::typed::TypedValue},
    types::construct_type::ConstructType
  },
  prelude::*,
  weak_handle,
};

pub mod errors;
pub mod frames;

pub use errors::polars_error_t;
pub use frames::polars_dataframe_t;

julia_module!{
  become julia_module_polars_init_fn;

  fn polars_version() -> StringRet;

  struct polars_error_t;
  in polars_error_t fn new(msg: JuliaString) -> CCallRefRet<polars_error_t> as polars_error_t;
  in polars_error_t fn message(&self) -> StringRet as polars_error_message;

  struct polars_dataframe_t;
  in polars_dataframe_t fn new_empty() -> CCallRefRet<polars_dataframe_t> as polars_dataframe_t;
  in polars_dataframe_t fn height(&self) -> usize as polars_dataframe_height;
}

pub fn polars_version() -> StringRet {
  leak_string(polars::VERSION)
}

pub(crate) fn leak_string<S: AsRef<str>>(s: S) -> StringRet {
  match weak_handle!() {
    Ok(handle) => {
      JuliaString::new(handle, s).leak()
    },
    Err(_) => panic!("Could not create weak handle to Julia."),
  }
}

pub(crate) fn leak_value<T: ConstructType + IntoJulia>(value: T) -> CCallRefRet<T> {
  match weak_handle!() {
    Ok(handle) => {
      CCallRefRet::new(TypedValue::new(handle, value).leak())
    },
    Err(_) => panic!("Could not create weak handle to Julia."),
  }
}
