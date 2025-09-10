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

pub use errors::polars_error_t;

julia_module!{
  become julia_module_polars_init_fn;

  fn polars_version() -> StringRet;

  struct polars_error_t;
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
