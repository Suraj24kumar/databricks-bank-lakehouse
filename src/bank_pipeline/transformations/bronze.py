"""Bronze: one Auto Loader streaming table per landing feed. Raw strings + lineage, append-only, replayable."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from bank_lakehouse.rules import RAW_COLUMNS

LANDING_PATH = spark.conf.get("bank.landing_path")  # /Volumes/<catalog>/<schema>/landing  (set in the bundle)


def define_bronze(feed: str) -> None:
    schema = StructType([StructField(c, StringType()) for c in RAW_COLUMNS[feed]])

    @dp.table(
        name=f"bronze_{feed}",
        comment=f"Raw `{feed}` rows from {LANDING_PATH}/{feed}/ via Auto Loader. Everything is STRING; bad values are caught in silver.",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(schema)
            .load(f"{LANDING_PATH}/{feed}/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


for _feed in RAW_COLUMNS:
    define_bronze(_feed)
