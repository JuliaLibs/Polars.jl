module DataTypes

import ..FFI: polars_value_type_t

abstract type DataType end
struct Null <: DataType end
struct Boolean <: DataType end
struct Int8 <: DataType end
struct Int16 <: DataType end
struct Int32 <: DataType end
struct Int64 <: DataType end
struct UInt8 <: DataType end
struct UInt16 <: DataType end
struct UInt32 <: DataType end
struct UInt64 <: DataType end
struct Float32 <: DataType end
struct Float64 <: DataType end
struct Decimal{P, S} <: DataType end
# unit can be :ns, :μs, :ms
struct DateTime{U} <: DataType
  time_zone::Union{String, Nothing}
end
struct Date <: DataType end
# unit must be :μs
struct Time{U} <: DataType end
# unit can be :ns, :μs, :ms
struct Duration{U} <: DataType end
struct List{T<:DataType} <: DataType
  inner::T
end
struct Array{T<:DataType, N} <: DataType
  inner::T
end
struct Unknown <: DataType
  tag::Symbol
  inner::polars_value_type_t
end

Decimal(P, S)::DataType = Decimal(Int(P), Int(S))
Decimal(P::Int, S::Int)::DataType = Decimal{P, S}()
Array(dt::DataType, n::Int)::DataType = Array{typeof(dt), n}(dt)
Base.precision(_::Decimal{P, S}) where {P, S} = P
scale(_::Decimal{P, S}) where {P, S} = S

function DataType(sym::Symbol; kwargs...)::DataType
  if sym === :Null
    return Null()
  elseif sym === :Boolean
    return Boolean()
  elseif sym === :Int8
    return Int8()
  elseif sym === :Int16
    return Int16()
  elseif sym === :Int32
    return Int32()
  elseif sym === :Int64
    return Int64()
  elseif sym === :UInt8
    return UInt8()
  elseif sym === :UInt16
    return UInt16()
  elseif sym === :UInt32
    return UInt32()
  elseif sym === :UInt64
    return UInt64()
  elseif sym === :Float32
    return Float32()
  elseif sym === :Float64
    return Float64()
  elseif sym === :Decimal
    # TODO: default precision and scale
    precision = get(kwargs, :precision, 10)
    scale = get(kwargs, :scale, 2)
    return Decimal(precision, scale)
  elseif sym === :Datetime
    unit = kwargs[:time_unit]
    time_zone = get(kwargs, :time_zone, nothing)
    return DateTime{unit}(time_zone)
  elseif sym === :Date
    return Date()
  elseif sym === :Time
    return Time{:μs}()
  elseif sym === :Duration
    unit = kwargs[:time_unit]
    return Duration{unit}()
  elseif sym === :List
    inner = convert(DataType, kwargs[:inner])
    return List(inner)
  elseif sym === :Array
    inner = convert(DataType, kwargs[:inner])
    n = convert(Int, kwargs[:size])
    return Array(inner, n)
  else
    throw(ArgumentError("Unimplemented data type symbol: $sym"))
  end
end

end # module DataTypes
