module DataTypes

abstract type DataType end
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

function DataType(sym::Symbol; kwargs...)::DataType
  if sym === :Bool
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
    # default precision and scale
    precision = get(kwargs, :precision, 10)
    scale = get(kwargs, :scale, 2)
    return Decimal{precision, scale}()
  else
    throw(ArgumentError("Unsupported data type symbol: $sym"))
  end
end

end # module DataTypes
