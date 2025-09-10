module Polars

include("ffi.jl")

export version
import .FFI: polars_error_t, polars_dataframe_t

version()::String = FFI.polars_version()

message(err::polars_error_t)::String = FFI.polars_error_message(err)

height(df::FFI.polars_dataframe_t)::UInt = FFI.polars_dataframe_height(df)

end # module Polars
