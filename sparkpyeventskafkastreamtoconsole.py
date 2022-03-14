from pyspark.sql import SparkSession

from pyspark.sql.functions import from_json, col, unbase64, base64, split

from pyspark.sql.types import StructField, StructType, StringType, FloatType, ArrayType, DateType

# make a spark application object

spark = SparkSession.builder.appName("kafkaEventsFile").getOrCreate()
#set loglevel to WARN
spark.sparkContext.setLogLevel('WARN')
# here use the spark application object,To read a Streaming DataFrame from the Kafka topic stedi-events as the source

kafkaEventsFileDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load() 



# cast  value column in the streaming dataframe as a STRING 


#value column in the streaming dataframe
kafkaEventsFileDF = kafkaEventsFileDF.selectExpr("cast(key as string) key","cast(value as string) value")

#  parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk

riskJSONFromRedisSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

kafkaEventsFileDF.withColumn("value",from_json("value",riskJSONFromRedisSchema ))\
             .select(col('value.customer'), col('value.score'), col('value.riskDate'))\
             .createOrReplaceTempView("CustomerRisk")



# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

#selecting the customer and the score from the temporary view "CustomerRisk"
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")




#sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----



customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafkastreaming.sh
# Verify the data looks correct 