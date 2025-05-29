from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession.builder.appName("WordCount").getOrCreate()

text_file = spark.read.text("gs://us-central1-learning-compos-dac16dfc-bucket/data/word_data.txt")
counts = text_file.withColumn('word', explode(split(text_file['value'], ' '))) \
    .groupBy('word') \
    .count()

counts.write.mode('overwrite')\
    .option('header', 'true') \
    .csv("gs://us-central1-learning-compos-dac16dfc-bucket/output")
