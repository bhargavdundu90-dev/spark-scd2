import os
import tempfile

import pytest

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Optional Iceberg integration tests. Point one of these at the Iceberg Spark runtime
# matching your Spark version, for example
#   SPARK_SCD2_ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
#   SPARK_SCD2_ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
ICEBERG_JAR = os.environ.get("SPARK_SCD2_ICEBERG_JAR")
ICEBERG_PACKAGES = os.environ.get("SPARK_SCD2_ICEBERG_PACKAGES")
ICEBERG_ENABLED = bool(ICEBERG_JAR or ICEBERG_PACKAGES)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("spark-scd2-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
    )
    if ICEBERG_ENABLED:
        warehouse = tempfile.mkdtemp(prefix="spark-scd2-iceberg-")
        if ICEBERG_JAR:
            builder = builder.config("spark.jars", ICEBERG_JAR)
        else:
            builder = builder.config("spark.jars.packages", ICEBERG_PACKAGES)
        builder = (
            builder.config(
                "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
        )
    session = builder.getOrCreate()
    yield session
    session.stop()
