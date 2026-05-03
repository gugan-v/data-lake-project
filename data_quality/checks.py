from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def validate(df: DataFrame):
    """
    Single-pass validation. Returns (good_df, bad_df) instead of raising.
    Bad rows are tagged with a reason and routed to quarantine — stream keeps running.
    """

    # Build a reason column in one pass (no repeated .count() scans)
    tagged = df.withColumn(
        "_invalid_reason",
        when(col("amount").isNull() | (col("amount") <= 0), lit("invalid_amount"))
        .when(col("status").isNull(), lit("null_status"))
        .when(col("country").isNull(), lit("null_country"))
        .otherwise(lit(None).cast("string"))
    )

    good_df = tagged.filter(col("_invalid_reason").isNull()).drop("_invalid_reason")
    bad_df  = tagged.filter(col("_invalid_reason").isNotNull())

    return good_df, bad_df
