module Polars

include("ffi.jl")

version()::String = FFI.polars_version()

import .FFI: polars_error_t, polars_column_t, polars_value_type_t

struct DataFrame
  inner::FFI.polars_dataframe_t
end

struct Column
  inner::polars_column_t
end

struct DataType
  inner::polars_value_type_t
end

DataFrame()::DataFrame = FFI.polars_dataframe_new_empty()
DataFrame(cols::Vector{Column})::DataFrame = FFI.polars_dataframe_from_cols([col.inner for col in cols])
Base.convert(::Type{DataFrame}, df::FFI.polars_dataframe_t) = DataFrame(df)
Base.unsafe_convert(::Type{FFI.polars_dataframe_t}, df::DataFrame) = df.inner
Base.show(io::IO, df::DataFrame) = FFI.polars_dataframe_show(df.inner, io)
Base.size(df::DataFrame) = FFI.polars_dataframe_height(df.inner)
height(df::DataFrame)::UInt = FFI.polars_dataframe_height(df.inner)
read_parquet(path::String)::DataFrame = FFI.polars_dataframe_read_parquet(path)
write_parquet(df::DataFrame, path::String)::Nothing = FFI.polars_dataframe_write_parquet(df.inner, path)

Column(name::String)::Column = FFI.polars_column_new_empty(name)
dtype(col::Column)::DataType = FFI.polars_column_dtype(col.inner)
Base.convert(::Type{Column}, col::polars_column_t) = Column(col)
Base.unsafe_convert(::Type{polars_column_t}, col::Column) = col.inner
Base.size(col::Column) = FFI.polars_column_len(col.inner)

Base.convert(::Type{DataType}, dtype::polars_value_type_t) = DataType(dtype)
Base.unsafe_convert(::Type{polars_value_type_t}, dtype::DataType) = dtype.inner
Base.show(dtype::DataType) = FFI.polars_value_type_display(dtype.inner)

end # module Polars
