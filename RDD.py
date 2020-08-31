import re

from pyspark.sql import SparkSession


def pp(s):
    decorator = "=" * 50
    print("\n\n{} {} {}".format(decorator, s, decorator))


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    sc = spark.sparkContext


    pp("Distributed collections")
    distCollection = sc.parallelize([1, 2, 3, 4])
    print(distCollection)
    localCollection = distCollection.collect()
    print(localCollection)


    pp("Reading files")
    poetry = sc.textFile('./poetry/')
    for s in poetry.take(5):
        print(s)

    poetryFiles = sc.wholeTextFiles('poetry')
    for f in poetryFiles.collect():
        print(f)

    # Sequence files
    import shutil
    shutil.rmtree('sequence', ignore_errors=True)
    poetryFiles.repartition(1).saveAsSequenceFile('sequence')
    sequenceFiles = sc.sequenceFile('sequence')
    for f in sequenceFiles.collect():
        print(f)


    pp("Transformations")
    print(poetry.map(lambda x: len(x)).take(5))

    def getWords(rdd):
        return rdd\
            .flatMap(lambda x: x.split(' '))\
            .map(lambda x: re.sub('\W', '', x.lower()))

    words = getWords(poetry)
    print(words.take(5))
    print(words.filter(lambda x: x[0] == 'п').take(5))
    print(words.sample(False, 0.05).take(5))

    # Passing functions
    def square(x):
        return x**2

    print(sc.parallelize([1, 2, 3]).map(square).collect())

    # Map partitions
    def enrich(connection, x):
        return "this is " + x

    def enrichPartition(xs):
        connection = 'open'
        enrichedXs = [enrich(connection, x) for x in xs]
        connection = 'close'
        return enrichedXs

    print(poetry.mapPartitions(enrichPartition).take(10))

    # Aggregations
    print(sc.parallelize([1, 3, 2, 3]).distinct().take(5))
    wordsByFirstLetter = words.map(lambda x: (x[0], x))
    print(wordsByFirstLetter.take(5))
    print(wordsByFirstLetter.reduceByKey(lambda a, b: a if len(a) > len(b) else b).take(5))
    print(wordsByFirstLetter.groupByKey().map(lambda x: (x[0], list(x[1]))).take(5))
    print(wordsByFirstLetter.aggregateByKey(
        0,
        lambda agg, x: agg + 1,
        lambda a, b: a + b
    ).take(5))

    pp("Actions")
    print(words.collect())
    print(words.first())
    print(words.take(10))
    print(words.count())
    print(words.reduce(lambda a, b: a if len(a) > len(b) else b))
    words.foreach(print)        # Bad
    for w in words.collect():   # Good
        print(w)


    pp("RDD manipulation")

    poetry1 = getWords(sc.textFile('poetry/part-0.txt'))
    poetry2 = getWords(sc.textFile('poetry/part-1.txt'))

    print(poetry1.intersection(poetry2).collect())
    print(poetry1.union(poetry2).collect())
    print(poetry1.zipWithIndex().take(5))

    def enumerateLines(rdd):
        return rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

    sideBySide = enumerateLines(sc.textFile('poetry/part-0.txt')) \
        .join(enumerateLines(sc.textFile('poetry/part-1.txt'))) \
        .take(5)
    for s in sideBySide:
        print(s)

    print(poetry1
          .map(lambda x: (x[0], x))
          .cogroup(poetry2.map(lambda x: (x[0], x)))
          .map(lambda x: (x[0], list(x[1][0]), list(x[1][1])))
          .take(5))


    pp("Cache")

    # Reads file and flatMap each time
    words.count()
    words.count()

    # words calculated only once
    wordsCached = words.cache()
    wordsCached.count()
    wordsCached.count()


    pp("Accumulators")

    numbers = sc.parallelize([1, 2, 3])

    # Bad example
    counter = 0

    def increment_counter(x):
        global counter
        counter += x

    numbers.foreach(increment_counter)
    print("Counter value: ", counter)

    # Good example
    accum = sc.accumulator(0)
    numbers.foreach(lambda x: accum.add(x))
    print("Accumulator value: ", accum.value)


    pp("Broadcast")

    priceDictionary = {"apple": 2.5, "orange": 3.2, "pineapple": 8.0}
    broadcastedDictionary = sc.broadcast(priceDictionary)
    print(sc.parallelize([{"item": "apple", "quantity": 3}])
          .map(lambda x: (x["item"], x["quantity"] * broadcastedDictionary.value[x["item"]]))
          .collect())


    spark.stop()













