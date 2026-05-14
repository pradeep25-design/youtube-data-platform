from pyspark.sql import SparkSession
from utils.config import get_spark_config


def create_spark_session(app_name=None):
    cfg      = get_spark_config()
    app_name = app_name if app_name else cfg["app_name"]

    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .master(cfg["master"])
        .config("spark.sql.session.timeZone", cfg["timezone"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.sql.warehouse.dir", cfg["warehouse_dir"])
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(cfg["log_level"])
    print(f"SparkSession started  : {app_name}")
    print(f"Spark version         : {spark.version}")
    print(f"Master                : {cfg['master']}")
    return spark


def stop_spark_session(spark):
    if spark:
        name = spark.sparkContext.appName
        spark.stop()
        print(f"SparkSession stopped  : {name}")


def test_spark_session():
    spark = create_spark_session(app_name="TamilNaduYouTube_Test")

    test_data = [
        ("v001", "Vijay New Movie Trailer",  "SunTV",    5000000, 250000),
        ("v002", "Ajith Kumar Interview",    "VijayTV",  3000000, 180000),
        ("v003", "Tamil Comedy 2025",        "ZeeTamil", 1500000,  90000),
        ("v004", "TN Politics Today",        "Polimer",   800000,  45000),
        ("v005", "Kollywood Latest News",    "NakkeeranTV", 500000, 30000),
    ]
    columns = ["video_id", "title", "channel", "views", "likes"]

    df = spark.createDataFrame(test_data, columns)
    print("\nSample Tamil Nadu YouTube DataFrame:")
    df.show(truncate=False)
    print(f"Total videos : {df.count()}")
    print(f"Total views  : {df.agg({'views': 'sum'}).collect()[0][0]:,}")
    stop_spark_session(spark)


if __name__ == "__main__":
    test_spark_session()
