from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.orderBy("count")

spark = SparkSession\
 .builder\
 .appName("DimActivityInit")\
 .getOrCreate()

#Load from Mongo
logDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/NewLog.json', format='json')
logDF = logDF.filter(logDF.filename != 'NEWLogBUIL 1024 _1094_SP42020.csv')

logDF = logDF.groupBy('Event name').count().withColumn('ActivitySK', row_number().over(w))
logDF = logDF.withColumnRenamed('count', 'NumberOfActivities')\
             .withColumnRenamed('Event name', 'ActivityName')

#Save Table
logDF.write.mode("overwrite").saveAsTable("unisadw.dimactivity")