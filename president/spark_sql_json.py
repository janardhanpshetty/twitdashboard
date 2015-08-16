#!/usr/bin/env python
from __future__ import print_function


import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

# Module constants
APP_NAME = "Spark SQL gets hourly data from hdfs, processes and puts to mongodb"


def main():
    if len(sys.argv) != 2:
        print("Usage: spark_sql_mongo.py </dir/dashboard/data/jsonfiles>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName=APP_NAME)
    sqlContext = SQLContext(sc)
    twitdf = sqlContext.read.json(sys.argv[1])
    twitdf.printSchema()
    # Register this DataFrame as a table.
    twitdf.registerTempTable("twittable")
    # Count of number of json object signifies count of num people    
    numdf = sqlContext.sql("SELECT count(*) as peoplecount from twittable")
    # print(numdf.take(1))
    # Convert Dataframe to Json document and save
    numdf.toJSON().saveAsTextFile('hourly_file.name')


if __name__ == "__main__":
    main()
