from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession\
 .builder\
 .appName("GenTables")\
 .getOrCreate()

DateDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/local/QID1109_20210913_202220_1.parq', format='parquet')
TimeDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/local/DimTime.parq', format='parquet')
GradeDf = spark.read.options(header='True', inferSchema='True', delimiter=',')\
                .csv("abfss://datalake@dus02store.dfs.core.windows.net/local/GradeScheme.csv")

GradeDf = GradeDf.withColumn('UpperBound', GradeDf.UpperBound.cast(IntegerType()))\
                .withColumn('LowerBound', GradeDf.LowerBound.cast(IntegerType()))\
                .withColumn('GradePoint', GradeDf.GradePoint.cast(DecimalType(3,1)))

DateDF.write.mode("overwrite").saveAsTable("unisadw.dimdate")
TimeDF.write.mode("overwrite").saveAsTable("unisadw.dimtime")
GradeDf.write.mode("overwrite").saveAsTable("unisadw.dimgrade")