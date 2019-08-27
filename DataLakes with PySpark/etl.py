import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import calendar
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('default', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_input, output_data):
    """
    The process song function takes in a desired path with the appropriate data, 
    reads it into a pyspark dataframe, and then converts the chosen information 
    (songs / artists) into separate parquet files into the desired paths.
    """
    
    # read song data file
    df = spark.read.json(song_input)
    
    #removing duplicates to ensure that the data is clean
    df = df.dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet('data/tests/songs_table.parquet')
    print('Songs table Parquet is finished')
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet('data/tests/artist_table.parquet')


def process_log_data(spark, log_input, output_data):
    """
    The process log function essentially takes an input path of data to be 
    read in pyspark. From there the function splits out the data into 
    several pyspark parquet files to be written to specific paths.
    """
    
    # read log data file
    df = spark.read.json(log_input)
    
    #removing duplicates to ensure that the data is clean
    df = df.dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    users.write.parquet('data/tests/users_table.parquet')

    #create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    
    #creating datetime column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    
    #creating number of the week column
    get_week = udf(lambda x: calendar.day_name[x.weekday()])
    
    #getting the weekday
    get_weekday = udf(lambda x: x.isocalendar()[1])
    
    #grabbing the hour
    get_hour = udf(lambda x: x.hour)
    
    get_day = udf(lambda x : x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)
    
    #creating columns using the UDF's above 
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    df = df.withColumn('datetime', get_datetime(df.ts))
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', get_hour(df.start_time))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    
    # extract columns to create time table
    time_table  = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet('data/tests/time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet('data/tests/songs_table.parquet')
    
    #join the tables together to have all info needed for the songplays table
    df = df.join(song_df, song_df.title == df.song)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet('data/tests/songplays_table.parquet')

def main():
    spark = create_spark_session()
#     input_data = "s3a://udacity-dend/"
    log_input = 'data/log_data/*.json'
    song_input = 'data/song_data/song_data/A/A/A/*.json'
    output_data = ""
    
    process_song_data(spark, song_input, output_data)    
    process_log_data(spark, log_input, output_data)


if __name__ == "__main__":
    main()