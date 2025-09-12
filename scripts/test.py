# %%
import polars as pl
from pathlib import Path
from datetime import datetime, date, time
workspace_dir = Path(__file__).parent.parent

# %%
df = pl.DataFrame({
  "col_null": pl.Series([None, None, None], dtype=pl.Null),
  "col_bool": pl.Series([True, False, True], dtype=pl.Boolean),
  "col_int8": pl.Series([1, 2, 3], dtype=pl.Int8),
  "col_int16": pl.Series([1, 2, 3], dtype=pl.Int16),
  "col_int32": pl.Series([1, 2, 3], dtype=pl.Int32),
  "col_int64": pl.Series([1, 2, 3], dtype=pl.Int64),
  "col_uint8": pl.Series([1, 2, 3], dtype=pl.UInt8),
  "col_uint16": pl.Series([1, 2, 3], dtype=pl.UInt16),
  "col_uint32": pl.Series([1, 2, 3], dtype=pl.UInt32),
  "col_uint64": pl.Series([1, 2, 3], dtype=pl.UInt64),
  "col_float32": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float32),
  "col_float64": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float64),
  "col_decimal": pl.Series([1.0, 2.0, 3.0], dtype=pl.Decimal(9, 3)),
  "col_string": pl.Series(["a", "b", "c"], dtype=pl.String),
  "col_datetime": pl.Series([datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)], dtype=pl.Datetime),
  "col_date": pl.Series([date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)], dtype=pl.Date),
  "col_time": pl.Series([time(12, 0, 0), time(13, 0, 0), time(14, 0, 0)], dtype=pl.Time),
  "col_duration": pl.Series([1000, 2000, 3000], dtype=pl.Duration),
  "col_binary": pl.Series([b"a", b"b", b"c"], dtype=pl.Binary),
  "col_list_int32": pl.Series([[1, 2], [3, 4], [5]], dtype=pl.List(pl.Int32)),
  "col_array_float64": pl.Series([[1.0], [3.0], [5.0]], dtype=pl.Array(pl.Float64, 1)),
})
df.write_parquet(workspace_dir / "test.parquet")

# %%
assert isinstance(df['col_null'].dtype, pl.Null)
assert isinstance(df['col_bool'].dtype, pl.Boolean)
assert isinstance(df['col_int8'].dtype, pl.Int8)
assert isinstance(df['col_int16'].dtype, pl.Int16)
assert isinstance(df['col_int32'].dtype, pl.Int32)
assert isinstance(df['col_int64'].dtype, pl.Int64)
assert isinstance(df['col_uint8'].dtype, pl.UInt8)
assert isinstance(df['col_uint16'].dtype, pl.UInt16)
assert isinstance(df['col_uint32'].dtype, pl.UInt32)
assert isinstance(df['col_uint64'].dtype, pl.UInt64)
assert isinstance(df['col_float32'].dtype, pl.Float32)
assert isinstance(df['col_float64'].dtype, pl.Float64)
assert isinstance(df['col_string'].dtype, pl.String)
assert isinstance(df['col_decimal'].dtype, pl.Decimal)
assert isinstance(df['col_datetime'].dtype, pl.Datetime)
assert isinstance(df['col_date'].dtype, pl.Date)
assert isinstance(df['col_time'].dtype, pl.Time)
assert isinstance(df['col_duration'].dtype, pl.Duration)
assert isinstance(df['col_binary'].dtype, pl.Binary)
assert isinstance(df['col_list_int32'].dtype, pl.List)
assert isinstance(df['col_array_float64'].dtype, pl.Array)

# %%
