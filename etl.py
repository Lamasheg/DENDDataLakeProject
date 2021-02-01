import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek




config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates a Spark Session
    
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process song data to build the songs and artists table and write them to parquet files
    
    Inputs:
    spark: spark session
    input_data: path to data files to extract the data
    output_data: path where the created tables will be stored
    
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/A/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet((output_data + 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet((output_data + 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Process log data to build the user, time and songsplays tables and write them to parquet files
    
    Inputs:
    spark: spark session
    input_data: path to data files to extract the data
    output_data: path where the created tables will be stored
    
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    actions_df = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist',
                                                         'sessionId', 'location','userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName','gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users.write.parquet((output_data + 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = actions_df.withColumn('timestamp', get_timestamp(actions_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('start_time', get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('start_time'))
    df = df.withColumn('day', dayofmonth('start_time'))
    df = df.withColumn('month', month('start_time'))
    df = df.withColumn('year', year('start_time'))
    df = df.withColumn('week', weekofyear('start_time'))
    df = df.withColumn('weekday', dayofweek('start_time'))
    
    time_table = df.select('start_time','hour','day','week','month','year','weekday').dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet((output_data + 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/*/*/*.json')
    df = df.join(song_df, song_df.title == df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('start_time','userId','level','song_id','artist_id','ssessionId',
                                'location','userAgent').withColumn('songplay_id',monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet((output_data + 'songplays/songplays.parquet'),'overwrite')
    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
