from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone


@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday

@udf(returnType=StringType())  
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df