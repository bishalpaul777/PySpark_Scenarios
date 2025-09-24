from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

#==== Need the dates when the status gets changed like ordered to dispatched ========

data = [
    ("1","1-Jan","Ordered"),
    ("1","2-Jan","dispatched"),
    ("1","3-Jan","dispatched"),
    ("1","4-Jan","Shipped"),
    ("1","5-Jan","Shipped"),
    ("1","6-Jan","Delivered"),
    ("2","1-Jan","Ordered"),
    ("2","2-Jan","dispatched"),
    ("2","3-Jan","Shipped")
]
df = spark.createDataFrame(data,["orderid","statusdate","status"])
df.show()

dispatcheddf = df.filter("status = 'dispatched'")
dispatcheddf.show()