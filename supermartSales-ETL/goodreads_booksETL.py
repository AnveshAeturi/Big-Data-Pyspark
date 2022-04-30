import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession.builder.appName('IRIS_ETL').master('local[*]').getOrCreate()
    return spark

if __name__ =="__main__":
    spark = get_spark()
    #iris_df = spark.read.csv('Iris.csv', header=True, inferSchema=True)
    #iris_df.show()

# spark-submit 

#SPARK_HOME/bin/spark-submit --master local[*] --packages com.databricks:spark-csv_2.10:1.5.0 goodread_booksETL\goodreads_booksETL.py

# create pyspark list of number from 0 to 100
data = spark.range(0, 100).collect()

# calculate the mean of the list
mean = data.mean()

#calculate median of the list
median = data.median()

# calculate the standard deviation of the list
std = data.std()


# calculate the variance of the list
variance = data.variance()

# calculate the skewness of the list
skewness = data.skewness()

