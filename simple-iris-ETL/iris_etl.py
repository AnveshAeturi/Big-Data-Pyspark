import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType, StructField, StructType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, desc, dense_rank, row_number, lit
from pyspark import StorageLevel
from pyspark.sql import functions as F

import os
import logging
logging.getLogger().setLevel(logging.INFO)

def get_spark():

    """Initialize SparkSession"""
    logging.info('Initializing Spark Session')
    spark = SparkSession.builder.appName('IRIS_ETL').master('local[*]').getOrCreate()
    return spark

def readFile(spark, file_path):
    """Read the CSV file"""
    try:
        iris_df = spark.read.csv(file_path, header=True, inferSchema=True)
        iris_df.show(5)
    except Exception  as e:
        logging.error('Error in loading File')
        logging.error(e)
    return iris_df

def  readFileWithSchema(spark, file_path):
    """Read the CSV file with Schema Declared"""
    schema = StructType([
        StructField('Id', IntegerType(), True),
        StructField('SepalLengthCm', FloatType(), True),
        StructField('SepalWidthCm', FloatType(), True),
        StructField('PetalLengthCm', FloatType(), True),
        StructField('PetalWidthCm', FloatType(), True),
        StructField('Species', StringType(), True)
    ])
    try:
        iris_df = spark.read.csv(file_path, header=True, schema=schema)
        iris_df.show(5)
    except Exception  as e:
        logging.error('Error in loading File')
        logging.error(e)
    return iris_df
    

    
def main():
    """Main Function to Execute ETL"""
    spark = get_spark()
    logging.info('Spark Session Initialized')

    # Read the CSV file
    #iris_df = readFile(spark, 'Iris.csv')      
    #logging.info('File Read Successfully')

    # Read File with Schema
    iris_df = readFileWithSchema(spark, 'Iris.csv')
    logging.info('File Read Successfully')
    iris_df.printSchema()

    window_spec = Window.partitionBy('Species').orderBy(desc('SepalLengthCm'))
    iris_df = iris_df.withColumn('rank_num', dense_rank().over(window_spec))

    iris_df = iris_df.withColumn('dqm', lit('DQM'))\
        .withColumn('Created_Date', F.current_date())\
            .withColumn('Created_By', lit(os.getlogin()))

    # This will aviod the OutOfMemoryError if input data is too large
    iris_df = iris_df.persist(StorageLevel.MEMORY_AND_DISK)
    print(iris_df.rdd.getNumPartitions())

    # Create column to check Null values in each row of the dataframe
    iris_df = iris_df.withColumn('DataQuality', F.when(col('SepalLengthCm').isNull(), 'Bad_records')\
        .when(col('SepalWidthCm').isNull(), 'Bad_records').when(col('PetalLengthCm').isNull(), 'Bad_records')\
        .when(col('PetalWidthCm').isNull(), 'Bad_records').when(col('Species').isNull(), 'Bad_records')\
        .otherwise('Good_reocrds')  )

    # Create a Temperary View Table to perfor SQL operations
    iris_df.createOrReplaceTempView('iris_tbl')
    logging.info('Table Created Successfully')
    logging.info('Executing SQL Query : ' + str(iris_df))
    spark.sql('SELECT * FROM iris_tbl limit 1').show()
    iris_df.show(5)

    # Create a folder to store the output files
    output_path = 'outputETL'
    if not os.path.exists(output_path):
        logging.info('Creating Output Folder')
        os.makedirs(output_path)
        
    #Writing to CSV
    iris_df.coalesce(1).write.csv(output_path + '/iris_output.csv', mode='overwrite', header=True)
    iris_df.coalesce(1).write.partitionBy('Species').mode('overwrite').csv(output_path + '/iris_output_part.csv', header=True)
    logging.info('CSV Written Successfully')
    logging.info('')

    #Writing to Parquet
    iris_df.coalesce(1).write.partitionBy('Species').mode('overwrite').parquet(output_path + '/iris_output_parquet')
    logging.info('Parquet Written Successfully')
    logging.info('')

if __name__ == "__main__":
    main()
    logging.info('ETL Completed Successfully')
