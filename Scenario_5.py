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
    (1, 'abc', 31, 'abc@gmail.com'),
    (2, 'def', 23, 'defyahoo.com'),
    (3, 'xyz', 26, 'xyz@gmail.com'),
    (4, 'qwe', 34, 'qwegmail.com'),
    (5, 'iop', 24, 'iop@gmail.com')
]

df1 = spark.createDataFrame(data,["id","name","age","email"])
df1.show()

data1 = [
    (11, 'jkl', 22, 'abc@gmail.com', 1000),
    (12, 'vbn', 33, 'vbn@yahoo.com', 3000),
    (13, 'wer', 27, 'wer', 2000),
    (14, 'zxc', 30, 'zxc.com', 2000),
    (15, 'lkj', 29, 'lkj@outlook.com', 2000)
]

df2 = spark.createDataFrame(data1,["id","name","age","email","salary"])
df2.show()

df3 = df1.withColumn("salary",lit(1000))
df3.show()

df4 = df3.union(df2)
df4.show()


finaldf = df4.filter("email like '%@%'")
finaldf.show()

