// Opaque wrapper type for a Polars Column
#[repr(C)]
pub struct Column {
    _private: [u8; 0],
}

impl Column {
    pub fn null() -> *mut Column {
        std::ptr::null_mut()
    }
}

pub type ColumnRef = *mut Column;
