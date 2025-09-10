use jlrs::{convert::into_julia::IntoJulia, data::{layout::valid_layout::ValidLayout, managed::{ccall_ref::{CCallRef, CCallRefRet}, string::StringRet, value::typed::TypedValue}, types::{abstract_type::IO, construct_type::ConstructType}}, error::JlrsError, inline_static_ref, prelude::*, weak_handle};


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

pub(crate) struct IOWrapper<'scope, 'data, T: Target<'scope>> {
  target: &'data T,
  io: &'data CCallRef<'scope, IO>,
}

impl<'scope, 'data, T: Target<'scope>> IOWrapper<'scope, 'data, T> {
  pub fn new(target: &'data T, io: &'data CCallRef<'scope, IO>) -> Self {
    Self { target, io }
  }
}

impl<'scope, 'data, T: Target<'scope>> std::io::Write for IOWrapper<'scope, 'data, T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    unsafe_write(self.target, self.io, buf)
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

/// unsafe_write(io::IO, ref, nbytes::UInt)
pub(crate) fn unsafe_write<'scope, T: Target<'scope>>(tgt: &T, io: &CCallRef<'scope, IO>, bytes: &[u8]) -> JlrsResult<()> {
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

pub type TypedVec<'scope, 'data, T> = TypedVector<'scope, 'data, TypedValue<'scope, 'data, T>>;

pub trait TypedVecExt<'scope, 'data, T> {
  fn extract_box<U, F>(&self, f: F) -> JlrsResult<Vec<U>>
  where
    F: Fn(&T) -> U,
    T: ConstructType + IntoJulia + ValidLayout;
}

impl<'scope, 'data, T> TypedVecExt<'scope, 'data, T> for TypedVec<'scope, 'data, T> {
  fn extract_box<U, F>(&self, f: F) -> JlrsResult<Vec<U>>
  where
    F: Fn(&T) -> U,
    T: ConstructType + IntoJulia + ValidLayout,
  {
    unsafe { self.managed_data() }
      .as_slice()
      .into_iter()
      .map(|c| match c.load(std::sync::atomic::Ordering::Relaxed) {
        Some(c) => {
          let val = unsafe { c.as_managed() };
          let val = unsafe { val.track_shared()? };
          Ok(f(&val))
        },
        None => Err(JlrsError::exception("Could not load column"))?,
      })
      .collect()
  }
}
