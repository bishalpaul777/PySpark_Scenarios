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



#====Write Query to get who are getting equal salary.

data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20", "09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11", "09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20", "09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20", "09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11", "09:00:00", "Admin")
]


df = spark.createDataFrame(data, ["Id","FirstName","LastName","Salary","JoiningDate","Time","Department"])
df.show()

countdf = df.groupBy("Salary").count().filter("count>1")
countdf.show()

finaldf = df.join(countdf,"Salary","inner").select("Id", "Firstname", "Lastname", "Salary", "joiningDate", "Department")
finaldf.show()


