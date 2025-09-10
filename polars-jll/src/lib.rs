use core::panic;

use jlrs::{
  convert::into_julia::IntoJulia, data::{
    managed::{ccall_ref::{CCallRef, CCallRefRet}, string::StringRet, value::typed::TypedValue},
    types::{abstract_type::IO, construct_type::ConstructType}
  }, error::JlrsError, inline_static_ref, prelude::*, weak_handle
};

pub mod errors;
pub mod frames;

pub type CCallResult<T> = JlrsResult<CCallRefRet<T>>;

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
  in polars_dataframe_t fn read_parquet(path: JuliaString) -> CCallResult<polars_dataframe_t> as polars_dataframe_read_parquet;
  in polars_dataframe_t fn write_parquet(&mut self, path: JuliaString) -> JlrsResult<()> as polars_dataframe_write_parquet;
  in polars_dataframe_t fn show(&self, io: CCallRef<IO>) -> JlrsResult<()> as polars_dataframe_show;
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

/// unsafe_write(io::IO, ref, nbytes::UInt)
pub(crate) fn unsafe_write<'tgt, T: Target<'tgt>>(tgt: T, io: CCallRef<IO>, bytes: &[u8]) -> JlrsResult<()> {
  tgt.local_scope::<_, 3>(|mut frame| {
    // unsafe_write(s::T, p::Ptr{UInt8}, n::UInt)
    let unsafe_write = inline_static_ref!(UNSAFE_WRITE_FUNCTION, Value, "Base.unsafe_write", frame);

    let arg0 = io.as_value()?;
    let arg1 = (bytes.as_ptr() as *mut u8).into_julia(&mut frame);
    let arg2 = bytes.len().into_julia(&mut frame);
    unsafe { unsafe_write.call(&mut frame, [arg0, arg1, arg2]) }
      .map_err(|_| JlrsError::exception("Failed to call unsafe_write"))?;
    Ok(())
  })
}
