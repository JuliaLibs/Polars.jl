module Polars

include("ffi.jl")

export version
import .FFI: polars_error_t, polars_dataframe_t

version()::String = FFI.polars_version()

message(err::polars_error_t)::String = FFI.polars_error_message(err)

height(df::FFI.polars_dataframe_t)::UInt = FFI.polars_dataframe_height(df)
read_parquet(path::String)::polars_dataframe_t = FFI.polars_dataframe_read_parquet(path)
write_parquet(df::polars_dataframe_t, path::String)::Nothing = FFI.polars_dataframe_write_parquet(df, path)
Base.show(io::IO, df::polars_dataframe_t)::Nothing = FFI.polars_dataframe_show(df, io)

end # module Polars
