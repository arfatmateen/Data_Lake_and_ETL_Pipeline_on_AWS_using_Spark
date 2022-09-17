import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id


# If you are running the notebook on EMR on EC2 then you dont need to set the evironment variables
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates an Apache Spark session.

    Returns
    -------
    spark:
        spark session to work with.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read and load song_data dataset.
    Extract appropriate columns for song and artist tables.
    Wrtie results (song and artist tables) into parquet files.

    Parameters
    ----------
    spark:
        spark session to perform task.
    input_data:
        Path to an S3 bucket to load data from.
    output_data:
        Path to an S3 bucket to save the data.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.selectExpr([
        'artist_id', 'artist_name AS name', 'artist_location AS location',
        'artist_latitude AS latitude', 'artist_longitude AS longitude'
    ])

    # drop duplicate records
    artists_table = artists_table.drop_duplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    Read and load log_data dataset.
    Extract appropriate columns for user, time and songplay tables.
    Wrtie results (user, time and songplay tables) into parquet files.

    Parameters
    ----------
    spark:
        spark session to perform task.
    input_data:
        Path to an S3 bucket to load data from.
    output_data:
        Path to an S3 bucket to save the data.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr([
        'userId AS user_id', 'firstName AS first_name', 'lastName AS last_name',
        'gender', 'level'
    ])

    # drop duplicate records
    users_table = users_table.drop_duplicates()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df =

    # create datetime column from original timestamp column
    get_datetime = udf(lambda t: str(datetime.fromtimestamp(t / 1000)))
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.withColumn('start_time', df.datetime) \
        .withColumn('hour', hour(df.datetime)) \
        .withColumn('day', dayofmonth(df.datetime)) \
        .withColumn('week', weekofyear(df.datetime)) \
        .withColumn('month', month(df.datetime)) \
        .withColumn('year', year(df.datetime)) \
        .withColumn('weekday', dayofweek(df.datetime))

    # drop duplicate records
    time_table = time_table.drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(os.path.join(
        output_data, 'time'),
        partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(
        os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df['artist'] == song_df['artist_name']) & (df['song'] == song_df['title']), 'inner') \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .selectExpr(['songplay_id', 'datetime AS start_time', 'userId AS user_id', 'level', 'song_Id AS song_id', 'artist_Id AS artist_id',
                     'sessionId AS session_id', 'location', 'userAgent AS user_agent', 'year(datetime) AS year', 'month(datetime) AS month'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    """
    Create spark session to work with.
    Process song data and creates appropriate tables (song, artist) and load it on S3 bucket.
    Process log data and creates appropriate tables (user, time, songplay) and load it on S3 bucket.
    """

    spark = create_spark_session()

    # Set the input_data variable to empty string if you want to use
    # the data available in local directories (log_data, song_data)
    input_data = "s3a://udacity-dend/"

    # Set the output_data variable to an output driectory where you want to save the parquet files
    output_data = "s3://ud-nd-dl-bucket/Sparkify/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
