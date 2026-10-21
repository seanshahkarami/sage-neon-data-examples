# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb",
#   "polars",
#   "pyarrow",
# ]
# ///
import duckdb

# This example shows how to use duckdb to create a Polars dataframe of aligned HMP155 and PTB330 data
# based on their 1 min rollup buckets.
#
# There are many ways to perform alignment and this is just one of many approaches.
df = duckdb.sql("""
with hmp155 as (
    select
        time_bucket(interval '1 min', readout_time) as readout_time_start,
        avg(temperature) as avg_temperature,
        avg(relative_humidity) as avg_relative_humidity,
        min(dew_point) as min_dew_point
    from '../neon/reading.sensor.hmp155.parquet'
    group by readout_time_start
    order by readout_time_start
),
ptb330a as (
    select
        time_bucket(interval '1 min', readout_time) as readout_time_start,
        avg(barometric_pressure) as avg_barometric_pressure
    from '../neon/reading.sensor.ptb330a.parquet'
    group by readout_time_start
    order by readout_time_start
)
select *
from hmp155
join ptb330a using (readout_time_start)
where readout_time_start between '2026-10-20 10:00:00' and '2026-10-20 11:00:00'
order by readout_time_start;
""").pl()

print(df)
