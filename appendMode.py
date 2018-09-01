from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
	SparkSession = SparkSession.builder.master("local")\
								.appName("SparkStramingAppendMode")\
								.getOrCreate()
	SparkSession.sparkContext.setLogLevel("ERROR")
	#schema for london crime dataset.
	# defined in a struct type , which is made up of structField array. True, means every field is nullable
	schema = StructType ([StructField("lsoa_code",StringType(),True),\
						  StructField("bourough",StringType(),True),\
						  StructField("major_category",StringType(),True),\
						  StructField("minor_category",StringType(),True),\
						  StructField("value",StringType(),True),\
						  StructField("year",StringType(),True),\
						  StructField("month", StringType(), True)])
								#every file has a header

 
	fileStreamDF = SparkSession.readStream.option("header", "true")\
								.schema(schema)\
								.csv("./data/droplocation/")
	print ("")
	print ("Is the stream ready?")
	print(fileStreamDF.isStreaming)

	print (" ")
	print ("Schema of the input stream: ")

	print (fileStreamDF.printSchema)

	trimmedDF = fileStreamDF.select(
										fileStreamDF.bourough,
										fileStreamDF.year,
										fileStreamDF.month,
										fileStreamDF.value)\
							.withColumnRenamed(
								"value",
								"convictions")

	#writing subst of data to console.

	#only new rows will be written ot data frame, and only 30 row per time.
	query = trimmedDF.writeStream.outputMode("append")\
					 .format("console")\
					 .option("truncate","false")\
					 .start()\
					 .awaitTermination()