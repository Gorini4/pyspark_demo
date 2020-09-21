from pyspark.sql.column import Column
from pyspark.sql.column import _to_java_column
from pyspark.sql.column import _to_seq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    sc = spark.sparkContext

    def udfIpToIntScalaWrapper(ipString):
        _ipToIntUDF = sc._jvm.CustomUDFs.ipToIntUDF()
        return Column(_ipToIntUDF.apply(_to_seq(sc, [ipString], _to_java_column)))

    df = spark.createDataFrame(["192.168.0.1"], "string").toDF("ip")

    df\
        .withColumn("ip_int_scala", udfIpToIntScalaWrapper(col("ip")))\
        .show()

