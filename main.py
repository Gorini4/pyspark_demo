import sys

from pyspark.sql import SparkSession
from operator import add


def main(inputFile):

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    lines = spark.read.text(inputFile).rdd.map(lambda r: r[0])

    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1])

    output = counts.collect()

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()


if __name__ == "__main__":
    # inputFile = "main.py"
    file = sys.argv[1]
    main(file)
