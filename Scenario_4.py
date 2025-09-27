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


#===Write a query to list the unique customer names in the custtab table, along with the number of addresses associated with each customer.


data = [
    ("1","Mark Ray","AB"),
    ("2","Peter Smith","CD"),
    ("1","Mark Ray","EF"),
    ("2","Peter Smith","GH"),
    ("2","Peter Smith","CD"),
    ("3","Kate","IJ")
]

df = spark.createDataFrame(data, ["custid","custname","address"])
df.show()

finaldf = df.groupBy("custid", "custname").agg(collect_set("address").alias("address")).orderBy("custid")
finaldf.show()