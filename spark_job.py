from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize SparkSession
    spark = SparkSession.builder.appName("S3ToRedshiftLoader").getOrCreate()

    # Set S3 and Redshift credentials
    s3_access_key = "<Enter key here>"
    s3_secret_key = "<Enter key here>"
    s3_bucket = "deltabase"
    s3_path = "bde_temp_storage"
    redshift_url = "jdbc:redshift://redshift-cluster-1.cpy70uk1sbp9.ap-south-1.redshift.amazonaws.com:5439/dev"
    redshift_user = "<Enter Username>"
    redshift_password = "<Enter password>"
    redshift_table = "Analysis.demotemp"
    aws_region = "ap-south-1"

    # Read data from S3
    s3_data = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("accessKey", s3_access_key) \
        .option("secretKey", s3_secret_key) \
        .option("region", aws_region) \
        .load("s3a://{}/{}/".format(s3_bucket, s3_path))

    # Write data to Redshift
    s3_data.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", redshift_table) \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("append") \
        .save()

    # Stop the SparkSession
    spark.stop()
