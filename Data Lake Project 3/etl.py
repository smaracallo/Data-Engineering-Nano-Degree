import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import songs_table_query, artists_table_query, log_filter_query, users_table_query, time_table_query, songplays_table_query


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """ 
    Description: Extract song data files from S3 and process the data creating a songs and artist table 
    and loading it back to a new s3 bucket

    """
    
    # get filepath to song data file
    song_data = f"{input_data}song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table 
    songs_table = spark.sql(songs_table_query)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "songs/songs.parquet", mode = "overwrite")
    
    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "artists/artists.parquet", mode = "overwrite")

def process_log_data(spark, input_data, output_data):
    
    """ 
    Description: Extract log data files from S3 and process the data creating a users, time and songplays table 
    and loading it back to a new s3 bucket

    """
    
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("song_logs") 
    
    # filter by actions for song plays
    df = spark.sql(log_filter_query)
    df.createOrReplaceTempView("song_logs")

    # extract columns for users table    
    users_table = spark.sql(users_table_query).dropDuplicates(["user_id", "level"])
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "users/users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = spark.sql(time_table_query)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "time/time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/songs.parquet")
    song_df.createOrReplaceTempView("songplays")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_table_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "songplays/songplays.parquet", mode = "overwrite")

def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
