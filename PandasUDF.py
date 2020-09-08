from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp")\
            .config('spark.sql.autoBroadcastJoinThreshold', 0)\
            .config('spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT', 1)\
            .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1)\
            .getOrCreate()

    crimeFacts = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("boston_crimes/crime.csv")

    offenseCodes = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("boston_crimes/offense_codes.csv")

    crimeFacts \
        .join(offenseCodes, offenseCodes.CODE == crimeFacts.OFFENSE_CODE) \
        .filter(offenseCodes.NAME.contains("ROBBERY")) \
        .groupBy(offenseCodes.NAME) \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    broadcastedCodes = broadcast(offenseCodes)

    crimeFacts \
        .join(broadcastedCodes, broadcastedCodes.CODE == crimeFacts.OFFENSE_CODE) \
        .filter(broadcastedCodes.NAME.contains("ROBBERY")) \
        .groupBy(broadcastedCodes.NAME) \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    slen = pandas_udf(lambda s: s.str.len(), IntegerType())

    @pandas_udf(StringType())
    def to_upper(s):
        return s.str.upper()

    @pandas_udf("integer", PandasUDFType.SCALAR)
    def add_one(x):
        return x + 1

    df = spark.createDataFrame([(1, "John Doe", 21)],
                                ("id", "name", "age"))
    df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")) \
        .show()



    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def normalize(pdf):
        v = pdf.v
        return pdf.assign(v=(v - v.mean()) / v.std())

    df.groupby("id").apply(normalize).show()

