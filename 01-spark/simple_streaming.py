from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("stream_word_count").getOrCreate()

lines_df = spark.readStream.format("socket").options("host", "localhost").option("port","9999").load()

words_df = lines_df.select(expr("explode(splirt(value, ' ')) as word"))
counts_df = words_df.groupBy("word").count()

word_count_query = counts_df.writeStream.format("console")\
                            .outputMode("complete")\
                            .option("checkpointLocation", "checkpoint")\
                            .start()

word_count_query.awaitTermination()