use core::panic;

use jlrs::{data::managed::string::StringRet, prelude::*, weak_handle};

julia_module!{
  become julia_module_polars_init_fn;

  fn polars_version() -> StringRet;
}

pub fn polars_version() -> StringRet {
  let version = polars::VERSION.to_string();
  match weak_handle!() {
    Ok(handle) => {
      let s = JuliaString::new(handle, version);
      s.leak()
    },
    Err(_) => panic!("Could not create weak handle to Julia."),
  }
}
