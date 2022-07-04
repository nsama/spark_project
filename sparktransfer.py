from pyspark.sql.session import SparkSession
from pyspark import SparkContext

sc =SparkContext("local","StreamTest")
spark=SparkSession.builder.config("spark.jars", "/home/sam/Downloads/postgresql-42.4.0.jar") \
    .appName("Spark test").master("local[*]").getOrCreate()  
spark.sparkContext.setLogLevel("ERROR")

#read data from table electricity
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "electricity") \
    .option("user", "postgres").option("password", "postgres").load()
 

#load data to table electricitytest
df.write.format("jdbc").options(
     url='jdbc:postgresql://localhost:5432/postgres',
     driver='org.postgresql.Driver',
     dbtable='electricitytest',
     user='postgres',
     password='postgres').mode('append').save()