module Polars

include("ffi.jl")

export version
import .FFI: polars_error_t, polars_column_t, polars_dataframe_t

version()::String = FFI.polars_version()

FFI.polars_error_t(msg::String)::polars_error_t = FFI.polars_error_new(msg)
message(err::polars_error_t)::String = FFI.polars_error_message(err)

FFI.polars_dataframe_t()::polars_dataframe_t = FFI.polars_dataframe_new_empty()
FFI.polars_dataframe_t(cols::Vector{polars_column_t})::polars_dataframe_t = FFI.polars_dataframe_from_cols(cols)
height(df::polars_dataframe_t)::UInt = FFI.polars_dataframe_height(df)
read_parquet(path::String)::polars_dataframe_t = FFI.polars_dataframe_read_parquet(path)
write_parquet(df::polars_dataframe_t, path::String)::Nothing = FFI.polars_dataframe_write_parquet(df, path)

FFI.polars_column_t(name::String)::polars_column_t = FFI.polars_column_new_empty(name)

Base.size(df::polars_dataframe_t) = FFI.polars_dataframe_height(df)
Base.size(col::polars_column_t) = FFI.polars_column_len(col)
Base.show(io::IO, df::polars_dataframe_t) = FFI.polars_dataframe_show(df, io)

end # module Polars
