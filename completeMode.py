from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
	
	sparkSession = SparkSession.builder.master("local")\
								.appName("sparkStreaming-completeMode")\
								.getOrCreate()
	sparkSession.sparkContext.setLogLevel("ERROR")

	schema = StructType ([StructField("lsoa_code",StringType(),True),\
						  StructField("bourough",StringType(),True),\
						  StructField("major_category",StringType(),True),\
						  StructField("minor_category",StringType(),True),\
						  StructField("value",StringType(),True),\
						  StructField("year",StringType(),True),\
						  StructField("month", StringType(), True)])
	
	fileStreamDF = sparkSession.readStream\
								.option("header","true")\
								.option("maxFilesPerTrigger","1")\
								.schema(schema)\
								.csv("./data/droplocation/")


	# grouping by borough, counting them and ordering by the count.
	recordsPerBorough = fileStreamDF.groupBy("major_category")\
									.count()\
									.orderBy("count",ascending=False)

	#writing.........

	query = recordsPerBorough.writeStream.outputMode("complete")\
										 .format("console")\
										 .option("trucate","true")\
										 .option("numRow",30)\
										 .start()\
										 .awaitTermination()
