from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import sys

if __name__ == "__main__":
    inPath = sys.argv[1]
    outPath = sys.argv[2]

    spark = SparkSession.builder.appName('greenflag').getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(inPath)
    rankedDf = df.select("ObservationDate","ScreenTemperature","Region").withColumn("rank", rank().over(
        Window.orderBy(desc("ScreenTemperature"))))
    rankedDf.write.parquet(outPath)
    newDf = spark.read.parquet(outPath)
    print("Which date was the hottest day?")
    newDf.select("ObservationDate").where("rank == 1").show()

    print("What was the temperature on that day?")
    newDf.select("ScreenTemperature").where("rank == 1").show()

    print("In which region was the hottest day?")
    newDf.select("Region").where("rank == 1").show()
