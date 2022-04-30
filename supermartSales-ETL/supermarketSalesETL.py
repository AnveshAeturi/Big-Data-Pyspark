import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType, StructField, StructType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel, storagelevel
from pyspark.sql import functions as F
from pyspark.sql.types import *

import pandas as pd
import os 
import logging
logging.getLogger().setLevel(logging.INFO)


def get_spark():
    """Initialize SparkSession"""
    logging.info('Initializing Spark Session')
    app = 'SUPERMART SLAES ETL'
    spark = SparkSession.builder.appName(app).master('local[*]').getOrCreate()
    return spark

def readFile(spark, file_path):
    """Read the CSV file"""
    try:
        salesdf = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f'The Size of the Data Frame is: {salesdf.count()}X{len(salesdf.columns)}')
        print(salesdf.columns)
        print(salesdf.dtypes)
        salesdf.show(1)
    except Exception  as e:
        logging.error('Error in loading File')
        logging.error(e)
    return salesdf

def get_num_distinct_records(salesdf):
    """Get the number of distinct records in each feature"""
    try:
        logging.info('Getting the number of distinct records in each feature')
        for i in salesdf:
            print(f'Number of Distinct Records in {i} is: {salesdf.select(i).distinct().count()}')
    except Exception as e:
        logging.error('Error in getting distinct records')
        logging.error(e)


def read_data(): 
    spark = get_spark()
    logging.info('Spark Session Initialized')
    # Read the CSV file
    salesdf = readFile(spark, 'supermarket_sales.csv')
    #Get Number of Distinct Records in each feature
    get_num_distinct_records(salesdf)
    return salesdf


def write_date(salesdf):

    folderName = 'outputETL'
    if not os.path.exists(folderName):
        os.mkdir(folderName)

    def to_pandas(salesdf):
        try:
            logging.info('Converting DataFrame to Pandas DataFrame')
            df = salesdf.toPandas()
            # #Createing archive Folder if file already existed
            # try:
            #     if os.path.exists(f'{folderName}/sales_archive.csv'):
            #         logging.info('Archive File Exists.  Creating a new one')
            #         os.rename(f'{folderName}/sales_archive.csv', f'{folderName}/sales_archive_old.csv')
            #         df.to_csv(f'{folderName}/sales_archive.csv', index=False)
            #         print(f'Data Frame is written to {folderName}/sales_archive.csv')
            # except Exception as e:
            #     logging.error('Error in creating archive folder')
            #     logging.error(e)    
            df.to_csv(folderName + '/supermarket_sales_pandas.csv', index=False, header=True)
            logging.info('Created CSV File Sucessfully')
            logging.info('')
        except Exception as e:
            logging.error('Error in writing to Pandas CSV')
            logging.error(e)

    def to_parquet(salesdf):
        try:
            try:
                logging.info('Converting DataFrame to Parquet')
                logging.info('Writing Data to Parquet File')
                salesdf.coalesce(1).write.partitionBy('City' ,'ProductLine').mode('overwrite')\
                    .parquet(folderName + '/supermarket_sales_parquet.parquet')
                logging.info('Created Parquet File Sucessfully')
                logging.info('')
            except Exception as e:
                logging.error('Error in writing to Parquet')
                logging.error(e)
            try:
                logging.info('Converting DataFrame to Partitioned CSV File')
                salesdf.coalesce(1).write.partitionBy('City' ,'ProductLine').mode('overwrite')\
                    .csv(folderName + '/supermarket_sales_parquet.csv', header=True)
                logging.info('Created Partioned CSV File Sucessfully')
                logging.info('')
            except Exception as e:
                logging.error('Error in writing to Partitioned CSV')
                logging.error(e)

        finally:
            logging.info('Writing Data to File Completed')
            logging.info('Exiting Program')
            logging.info('')
    to_pandas(salesdf)
    to_parquet(salesdf)


if __name__ == '__main__':
    salesdf = read_data()
    logging.info('Data Load Completed')

    salesdf = salesdf.persist(StorageLevel.MEMORY_ONLY)

    ''' Thes are the features that we are going to use for the analysis
    ['Invoice ID', 'Branch', 'City', 'Customer type', 'Gender', 'Product line', 'Unit price', 
    'Quantity', 'Tax 5%', 'Total', 'Date', 'Time', 'Payment', 'cogs', 'gross margin percentage', 
    'gross income', 'Rating']
    '''
    # Convert the Column Names into Valid Format 
    salesdf = salesdf.withColumnRenamed('Invoice ID', 'InvoiceId').withColumnRenamed('Customer type', 'CustomerType')\
                    .withColumnRenamed('Product line', 'ProductLine').withColumnRenamed('Unit price', 'UnitPrice')\
                    .withColumnRenamed("Tax 5%", 'Tax5%').withColumnRenamed('cogs', 'Cogs')\
                    .withColumnRenamed('gross margin percentage', 'GrossMarginPercentage')\
                    .withColumnRenamed('gross income', 'GrossIncome')

    # Filter InvoiceId and remove '-' from the InvoiceId
    salesdf = salesdf.withColumn('InvoiceId', regexp_replace('InvoiceId', '-', ''))

    # Convert the Date and Time Columns into DateTime Format
    '''
        The Avaialble Date Formats are: MM-dd-yyyy and 
        M/dd/yyyy : M is the month which is single Valueswhich can be 1-12
    '''
    salesdf = salesdf.withColumn('NewDate',
     F.coalesce(
         to_date(salesdf['Date'], 'M/dd/yyyy'), 
         to_date(salesdf['Date'], 'MM-dd-yyyy') 
         ))    #.drop('Date')


    # # Convert the Time Columns into DateTime Format
    # salesdf = salesdf.withColumn('NewTime', to_timestamp(salesdf['Time'], 'HH:mm:ss'))\
    #                 .drop('Time')

    # Create a new Column for the DateTime Column

    salesdf = salesdf.withColumn('NewDate', col('NewDate').cast(StringType()))\
                    .withColumn('Time', col('Time').cast(StringType()))
    #date_time_col = udf(lambda x, y: concat(x, ' ', y), StringType())
    salesdf = salesdf.withColumn('DateTime', concat(col('NewDate'), lit(' '), col('Time')))

    
    # Convert the Date and Time Columns into DateTime Format
    salesdf = salesdf.withColumn('NewDate', col('NewDate').cast(DateType()))

    # Create Quarter of Year Column
    salesdf = salesdf.withColumn('QuarterOfYear', F.quarter(salesdf['NewDate']))

    # Create Month and Year Columns
    salesdf = salesdf.withColumn('Month', F.month(salesdf['NewDate']))\
                    .withColumn('Year', F.year(salesdf['NewDate']))


    # Sum of Total and Gross Income Monthly Wise
    cols_mnthly_sum = ['Year', 'Month'] 
    window_spec_MonthSum = Window.partitionBy(*cols_mnthly_sum).orderBy('NewDate')
    salesdf = salesdf.withColumn('QuantityMonthSum', sum('Quantity').over(window_spec_MonthSum))\
                    .withColumn('TotalMonthSum', sum('Total').over(window_spec_MonthSum))\
                    .withColumn('GrossIncomeMonthSum', sum('GrossIncome').over(window_spec_MonthSum))

    # Sum of Total and Gross Income Quarterly Wise per year
    cols_quarterly_sum = ['Year', 'QuarterOfYear']
    window_spec_QuarterSum = Window.partitionBy(*cols_quarterly_sum).orderBy('NewDate')
    salesdf = salesdf.withColumn('QuantityQTDSum', sum('Quantity').over(window_spec_QuarterSum))\
                    .withColumn('TotalQuarterSum', sum('Total').over(window_spec_QuarterSum))\
                    .withColumn('GrossIncomeQuarterSum', sum('GrossIncome').over(window_spec_QuarterSum))

    # Cumulative Sum of the Quantity, Total,  Column for Year sale analysis

    window_spec = Window.partitionBy('ProductLine').orderBy('NewDate')
    salesdf = salesdf.withColumn('QuantityCumSum', F.sum('Quantity').over(window_spec))\
                    .withColumn('TotalCumSum', F.sum('Total').over(window_spec))\
                    .withColumn('CogsCumSum', F.sum('Cogs').over(window_spec))\
                    .withColumn('GrossMarginPercentageCumSum', F.sum('GrossMarginPercentage').over(window_spec))\
                    .withColumn('GrossIncomeCumSum', F.sum('GrossIncome').over(window_spec))
    
    # # Create Week and Day Column
    # salesdf = salesdf.withColumn('Week', F.weekofyear(salesdf['NewDate']))\
    #                 .withColumn('Day', F.dayofweek(salesdf['NewDate']))

    # # Create Hour and Minute Columns
    # salesdf = salesdf.withColumn('Hour', F.hour(salesdf['Time']))\
    #                 .withColumn('Minute', F.minute(salesdf['Time'])) 
    
    # Create Day of Week Column
    salesdf = salesdf.withColumn('DayOfWeek', F.dayofweek(salesdf['NewDate']))

    # Create Day of Month Column
    salesdf = salesdf.withColumn('DayOfMonth', F.dayofmonth(salesdf['NewDate']))

    # Create Day of Year Column
    salesdf = salesdf.withColumn('DayOfYear', F.dayofyear(salesdf['NewDate']))

    # Create Week of Year Column
    salesdf = salesdf.withColumn('WeekOfYear', F.weekofyear(salesdf['NewDate']))
    
    # Create Data and sign column 
    salesdf = salesdf.withColumn('CreatedDate', current_date()).withColumn('CreatedBy', lit(os.getlogin()))

    salesdf.show(5)
    salesdf.printSchema()

    logging.info('Data Transformation Completed')
    logging.info('')
    logging.info('Initiated writing Data to File')
    write_date(salesdf)

    logging.info('ETL Completed Successfully')


