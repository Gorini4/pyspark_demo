from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def pp(s):
    decorator = "=" * 50
    print("\n\n{} {} {}".format(decorator, s, decorator))


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    crimeFacts = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("boston_crimes/crime.csv")

    crimeFacts.show(truncate=False)

    crimeFacts.printSchema()


    crimeFacts.select("hour", col("hour") + 1).show()


    crimeFacts.createOrReplaceTempView("crimes")
    spark.sql("select INCIDENT_NUMBER, DISTRICT from crimes limit 10").show()


    crimeFacts.select(mean("Lat").alias("avg_lat")).show()
    crimeFacts.select(approx_count_distinct("DISTRICT")).show()
    crimeFacts.select(stddev("Lat")).show()


    from pyspark.sql.window import Window
    import pyspark.sql.functions as func

    windowSpec = Window.partitionBy("DISTRICT") \
        .orderBy(col("Lat").desc())

    crimeFacts.select("DISTRICT", "Lat",
        func.max("Lat").over(windowSpec).alias("max_lat")).show()


    print(crimeFacts.rdd.take(5))


    from pyspark.sql.types import LongType
    from pyspark.sql.functions import udf

    def squared_typed(s):
        return s * s

    squared_udf = udf(squared_typed, LongType())

    crimeFacts.select("hour", squared_udf("hour")).show()

    offenseCodes = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("boston_crimes/offense_codes.csv")
    offenseCodes.show()

    robberyStats = crimeFacts\
        .join(offenseCodes, offenseCodes.CODE == crimeFacts.OFFENSE_CODE)\
        .filter(offenseCodes.NAME.contains("ROBBERY"))\
        .groupBy(offenseCodes.NAME)\
        .count()\
        .orderBy(col("count").desc())

    robberyStats.show(truncate=False)

    robberyStats.explain(True)







