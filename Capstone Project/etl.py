import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType

# The date format string preferred to our work here: YYYY-MM-DD
date_format = "%Y-%m-%d"

# The AWS key id and password are configured in a configuration file "dl.cfg"
config = configparser.ConfigParser()
config.read('dl.cfg')

# Reads and saves the AWS access key information and saves them in a environment variable
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creates a session with Spark, the entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .enableHiveSupport().getOrCreate()
    
    return spark

def read_data(spark, input_path):
    """
    Loads data from a data source using the pyspark module and returns it as a spark 'DataFrame'.
    
    """
    return spark.read.option("header",True).csv(input_path)

def immigration_data(spark, input_path="immigration_data_sample.csv", output_data = "s3a://data-capstone-final/",
                         partitionBy = ["i94yr", "i94mon"], header=True):
    """
    Reads the immigration dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter 
    out_put path.

    """
    
    # Loads the immigration dataframe using Spark
    immigration_df = read_data(spark, input_path=input_path, )
    
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count', 'biryear', 'dtaddto', 'dtadfile', 'depdate', 'fltno', 'admnum']
    
    date_cols = ['arrdate', 'depdate']
    
    high_null_cols = ["visapost", "occup", "entdepu", "insnum"]
    
    irrelevant_cols = ["_c0", "count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    
    # Convert columns read as string to integer
    for col in int_cols:
        immigration_df = immigration_df.withColumn(col, immigration_df[col].cast(IntegerType()))
    
    # immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    # Convert SAS date to a datetime in the format of YYYY-MM-DD
    for col in date_cols:
        immigration_df = immigration_df.withColumn(col, convert_date_udf(immigration_df[col]))
    
    # Drop high null columns and irrelevant columns
    immigration_df = immigration_df.drop(*high_null_cols)
    immigration_df = immigration_df.drop(*irrelevant_cols)
    
    # Create a new columns to store the length of the visitor stay in the US
    # immigration = immigration.withColumn('stay', date_diff_udf(immigration.arrdate, immigration.depdate))
    # immigration = cast_type(immigration, {'stay': IntegerType()})
    immigration_df = immigration_df.withColumn('staylength', date_diff_udf(immigration_df['arrdate'], immigration_df['depdate']))
    
    immigration_df = immigration_df.withColumnRenamed('i94yr','year') \
                                   .withColumnRenamed('i94mon', 'month') \
                                   .withColumnRenamed('i94cit', 'city_code') \
                                   .withColumnRenamed('i94res', 'country_code') \
    
    immigration_df.write.partitionBy("i94yr", "cicid").parquet(path = output_data + "capstone/immigration.parquet", mode = "overwrite")
    
    return immigration_df 
    
def demographics_data(spark, input_path="us-cities-demographics.csv", output_data = "s3a://data-capstone-final/", partitionBy = ["i94yr", "i94mon"], header=True):
    
    demographics_df = spark.read.options(header=True, inferSchema=True, sep=';').csv("us-cities-demographics.csv")
    
    demographics_df = demographics_df.withColumnRenamed('Median Age','median_age') \
            .withColumnRenamed('Male Population', 'male_population') \
            .withColumnRenamed('Female Population', 'female_population') \
            .withColumnRenamed('Total Population', 'total_population') \
            .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
            .withColumnRenamed('Foreign-born', 'foreign_born') \
            .withColumnRenamed('Average Household Size', 'average_household_size') \
            .withColumnRenamed('State Code', 'state_code')

    demographics_df = demographics_df.withColumn('id', monotonically_increasing_id())
    
    demographics_df.write.parquet(path = output_data + "capstone/demographics.parquet", mode = "overwrite")

def udf_date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, "%Y-%m-%d")
        b = datetime.strptime(date2, "%Y-%m-%d")
        delta = b - a
        return delta.days
    
# User defined functions using Spark udf wrapper function to convert SAS dates into string dates in the format YYYY-MM-DD, to capitalize the first letters of the string and to calculate the difference between two dates in days.
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
capitalize_udf = udf(lambda x: x if x is None else x.title())
date_diff_udf = udf(udf_date_diff)
convert_date_udf = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)


def main():
    spark = create_spark_session()
    
    # Perform ETL process for the Immigration dataset generating immigration and date tables and save them in the S3 bucket indicated in the output_path parameters.
    immigration = immigration_data(spark, input_path="immigration_data_sample.csv", output_data = "s3a://data-capstone-final/",
                         partitionBy = ["year", "nonth"], columns_to_save='*', header=True)
    
if __name__ == "__main__" :
    main()
    
    
    
