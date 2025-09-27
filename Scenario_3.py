from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\HP\.jdks\corretto-1.8.0_462'        #  <----- 🔴JAVA PATH🔴

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

#============== ⬆️ Main Code ⬆️ =======================



data = [
    ("1111","2021-01-15","10"),
    ("1111","2021-01-16","15"),
    ("1111","2021-01-17","30"),
    ("1112","2021-01-15","10"),
    ("1112","2021-01-15","20"),
    ("1112","2021-01-15","30")
]


df = spark.createDataFrame(data,["sensorid","timestamp","values"])
df.show()

windowdf = Window.partitionBy("sensorid").orderBy("values")          # partitioned the sensorid column for gouping

nextdf = df.withColumn("nextvalues", lead("values", 1).over(windowdf))   # adding new column

filterdf = nextdf.filter(col("nextvalues").isNotNull())             # filter out the null column

finaldf = filterdf.withColumn("values", expr("nextvalues-values").cast("int")).drop("nextvalues").orderBy(col("sensorid"))
finaldf.show()
