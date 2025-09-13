use jlrs::{convert::{into_julia::IntoJulia, unbox::Unbox}, data::{layout::valid_layout::ValidLayout, managed::{ccall_ref::{CCallRef, CCallRefRet}, named_tuple::NamedTuple, string::StringRet, symbol::SymbolRet, value::typed::TypedValue, Weak}, types::{abstract_type::IO, construct_type::ConstructType, typecheck::Typecheck}}, error::JlrsError, inline_static_ref, prelude::*, weak_handle};

use crate::errors::JuliaPolarsError;


pub(crate) fn leak_symbol(s: &'static str) -> SymbolRet {
  match weak_handle!() {
    Ok(handle) => {
      Symbol::new(&handle, s).leak()
    },
    Err(_) => JuliaPolarsError::WeakHandleError("leak_symbol").panic(),
  }
}

pub(crate) fn leak_string<S: AsRef<str>>(s: S) -> StringRet {
  match weak_handle!() {
    Ok(handle) => {
      JuliaString::new(handle, s).leak()
    },
    Err(_) => JuliaPolarsError::WeakHandleError("leak_string").panic(),
  }
}

pub(crate) fn leak_value<T: ConstructType + IntoJulia>(value: T) -> CCallRefRet<T> {
  match weak_handle!() {
    Ok(handle) => {
      CCallRefRet::new(TypedValue::new(handle, value).leak())
    },
    Err(_) => JuliaPolarsError::WeakHandleError("leak_value").panic(),
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

pub(crate) trait JuliaNamedTupleExt<'scope, 'data> {
  fn get_value(&self, handle: &impl Target<'scope>, key: &str) -> JlrsResult<Value<'scope, 'data>>;
}

impl<'scope, 'data> JuliaNamedTupleExt<'scope, 'data> for NamedTuple<'scope, 'data> {
  fn get_value(&self, handle: &impl Target<'scope>, key: &str) -> JlrsResult<Value<'scope, 'data>> {
    let Some(v) = self.get(handle, key) else {
      return Err(JlrsError::exception(format!("Missing {}", key)))?;
    };
    Ok(unsafe { v.as_value() })
  }
}

#[allow(unused)]
pub(crate) trait JuliaValueExt<'scope, 'data> {
  fn _as_value(&self) -> JlrsResult<Value<'scope, 'data>>;
  fn as_cast<T: Managed<'scope, 'data> + Typecheck>(&self) -> JlrsResult<T> {
    self._as_value()?.cast()
  }
  fn as_cast_opt<T: Managed<'scope, 'data> + Typecheck>(&self) -> JlrsResult<Option<T>> {
    match self._as_value() {
      Ok(v) => {
        if v.is::<Nothing>() {
          return Ok(None);
        }
        Ok(Some(v.cast()?))
      },
      Err(_) => Ok(None),
    }
  }
  fn as_unbox<T: Unbox<Output=T> + Typecheck>(&self) -> JlrsResult<T> {
    let v = self._as_value()?;
    v.unbox::<T>()
  }
}

impl<'scope, 'data> JuliaValueExt<'scope, 'data> for Weak<'scope, 'data, Value<'scope, 'data>> {
  fn _as_value(&self) -> JlrsResult<Value<'scope, 'data>> {
    Ok(unsafe { self.as_value() })
  }
}

impl<'scope, 'data> JuliaValueExt<'scope, 'data> for CCallRef<'scope, Value<'scope, 'data>> {
  fn _as_value(&self) -> JlrsResult<Value<'scope, 'data>> {
    self.as_value()
  }
}

impl<'scope, 'data> JuliaValueExt<'scope, 'data> for Value<'scope, 'data> {
  fn _as_value(&self) -> JlrsResult<Value<'scope, 'data>> {
    Ok(self.clone())
  }
}

pub(crate) trait CCallRefExt<'scope, T> {
  fn tracked_map<'borrow, F: FnOnce(&T) -> U, U>(&'borrow self, f: F) -> JlrsResult<U>;
}

impl<'scope, T: ConstructType + ValidLayout> CCallRefExt<'scope, T> for CCallRef<'scope, TypedValue<'scope, 'static, T>> {
  fn tracked_map<'borrow, F: FnOnce(&T) -> U, U>(&'borrow self, f: F) -> JlrsResult<U> {
    let v = self.as_value()?;
    let v = v.track_shared::<T>()?;
    Ok(f(&v))
  }
}
