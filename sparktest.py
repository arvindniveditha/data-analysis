import findspark
import os
import configparser
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
def main():
    # sc = SparkSession.builder \
    #     .master("local") \
    #     .appName("Word Count") \
    #     .config("spark.some.config.option", "some-value") \
    #     .getOrCreate()

    # l = [('Alice', 1)]
    # rdd = spark.createDataFrame(l).collect()
    # print(rdd)
    # sc = SparkContext('local')

    config = configparser.ConfigParser()

    config.read_file(open('../config.cfg'))
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    country_data_path = config['S3']['COUNTRY_DATA']
    # country_notes_path = config['S3']['COUNTRY_NOTES_DATA']
    # foot_notes_path = config['S3']['FOOT_NOTES_DATA']
    series_path = config['S3']['SERIES_DATA']
    # series_notes_path = config['S3']['SERIES_NOTES_DATA']
    indicators_path = config['S3']['INDICATORS_DATA']
    # output_path = config['S3']['OUTPUT']
    print(indicators_path)

    sc = SparkSession.builder.config('spark.jars.packages',
                                        'org.apache.hadoop:hadoop-aws:2.6.4').getOrCreate()

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",
                                         os.environ['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                         os.environ['AWS_SECRET_ACCESS_KEY'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    indicators_data = sc.read.csv(indicators_path, header=True)
    country_data = sc.read.csv(country_data_path, header=True)
    series_data = sc.read.csv(series_path, header=True)
    print(indicators_data.head(5))

if __name__ == '__main__':
    main()