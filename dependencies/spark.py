"""
spark.py 
~~~~~~~~

Module containg helper function for use with Apache Spark
"""

from pyspark.sql import SparkSession

def start_spark(app_name='ff_tiers_etl'):
    spark_builder = (
        SparkSession
        .builder
        .appName(app_name))

    spark_sess = spark_builder.getOrCreate()
    return spark_sess
