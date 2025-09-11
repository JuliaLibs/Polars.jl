# %%
import polars as pl

# %%
df = pl.DataFrame({
  "mycol": pl.Series([1, 2, 3], dtype=pl.Int64),
  "decimal": pl.Series([1.0, 2.0, 3.0], dtype=pl.Decimal(9, 3)),
})
df.write_parquet("test.parquet")

# %%
d = df['decimal'].dtype
assert isinstance(d, pl.DataType)

# %%
