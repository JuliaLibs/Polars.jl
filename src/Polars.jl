module Polars

include("ffi.jl")
include("datatype.jl")

version()::String = FFI.polars_version()

import .DataTypes: DataType
import .FFI: polars_error_t, polars_value_type_t

struct DataFrame
  inner::FFI.polars_dataframe_t
end

struct Column
  inner::FFI.polars_column_t
end

DataFrame()::DataFrame = FFI.polars_dataframe_new_empty()
DataFrame(cols::Vector{Column})::DataFrame = FFI.polars_dataframe_from_cols([col.inner for col in cols])
Base.convert(::Type{DataFrame}, df::FFI.polars_dataframe_t) = DataFrame(df)
Base.unsafe_convert(::Type{FFI.polars_dataframe_t}, df::DataFrame) = df.inner
Base.show(io::IO, df::DataFrame) = FFI.polars_dataframe_show(df.inner, io)
Base.size(df::DataFrame) = FFI.polars_dataframe_height(df.inner)
Base.getindex(df::DataFrame, name::String)::Column = FFI.polars_dataframe_get_column(df.inner, name)
height(df::DataFrame)::UInt = FFI.polars_dataframe_height(df.inner)
read_parquet(path::String)::DataFrame = FFI.polars_dataframe_read_parquet(path)
write_parquet(df::DataFrame, path::String)::Nothing = FFI.polars_dataframe_write_parquet(df.inner, path)
get_column(df::DataFrame, name::String)::Column = FFI.polars_dataframe_get_column(df.inner, name)

Column(name::String)::Column = FFI.polars_column_new_empty(name)
Base.convert(::Type{Column}, col::FFI.polars_column_t) = Column(col)
Base.unsafe_convert(::Type{FFI.polars_column_t}, col::Column) = col.inner
Base.size(col::Column) = FFI.polars_column_len(col.inner)
Base.getindex(col::Column, idx::Integer)::Any = FFI.polars_column_get(col.inner, convert(UInt, idx) - 1)
dtype(col::Column)::DataType = FFI.polars_column_dtype(col.inner)
name(col::Column)::String = FFI.polars_column_name(col.inner)
null_count(col::Column)::UInt = FFI.polars_column_null_count(col.inner)

function Base.convert(::Type{DataType}, dtype::FFI.polars_value_type_t)::DataType
  sym = FFI.polars_value_type_symbol(dtype)
  kwargs = FFI.polars_value_type_kwargs(dtype)
  println("Converting dtype: ", sym, " with kwargs: ", kwargs)
  try
    return DataTypes.DataType(sym; kwargs...)
  catch e
    if e isa ArgumentError && occursin("Unimplemented data type symbol", e.msg)
      println("Error converting dtype: ", e, sym, kwargs)
      return DataTypes.Unknown(sym, dtype)
    end
    rethrow(e)
  end
end

end # module Polars
