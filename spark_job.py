from pyspark.sql import SparkSession
import pandas as pd
from smart_open import smart_open
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')


def run_spark():
    # Set S3 and Redshift credentials
    s3_access_key = "AKIAT2LY7JPAZCUPXUE3"
    s3_secret_key = "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"
    s3_bucket = "deltabase"
    s3_path = "bde_temp_storage/scores.csv"
    redshift_url = "jdbc:redshift://redshift-cluster-1.cpy70uk1sbp9.ap-south-1.redshift.amazonaws.com:5439/dev"
    redshift_user = "awsuser"
    redshift_password = "Messi_1812"
    redshift_table = "Analysis.demotemp"
    aws_region = "ap-south-1"
    spark = SparkSession.builder \
        .appName("S3 to Redshift Job") \
        .config("spark.jars.packages", "com.amazon.redshift:redshift-jdbc42:2.1.0.14")\
        .config("spark.jars.packages", "com.databricks:spark-redshift_2.11:2.0.1")\
        .getOrCreate()

    # Read CSV data from S3
    path_twitter = 's3://{}:{}@{}/{}'.format(s3_access_key, s3_secret_key,
                                     'deltabase', 'bde_temp_storage/refined_tweets.csv')
    df_twitter = pd.read_csv(smart_open(path_twitter))
    path_finance = 's3://{}:{}@{}/{}'.format(s3_access_key, s3_secret_key,
                                     'deltabase', 'bde_temp_storage/refined_stocks.csv')
    df_finance = pd.read_csv(smart_open(path_finance))
    df = pd.merge(df_twitter,df_finance,left_on='created_at',right_on='Datetime')
    print(df)
    df = spark.createDataFrame(df)
# Write data to Redshift
    df.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", redshift_table) \
        .option("user", redshift_user) \
        .option("driver", 'com.amazon.redshift.jdbc.Driver')\
        .option("password", redshift_password) \
        .option("aws_iam_role", "S3toRedshiftEnabler") \
        .mode("append") \
        .save()

    # Stop the Spark session
    spark.stop()


run_spark()
