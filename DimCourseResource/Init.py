from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.orderBy("count")

spark = SparkSession\
 .builder\
 .appName("DimCourseResourceInit")\
 .getOrCreate()

resourceDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/learnonline.json', format='json')
resourceDF = resourceDF.groupBy(['Year', 'ActivityResourceName', 'ActivityResourceType', 'AssessmentMethod', 'CatalogNumber',
                                'CourseID', 'CourseName', 'SubjectArea', 'TermCode']).count()

resourceDF.withColumn('ResourceSK', row_number().over(w)).write.mode("overwrite").saveAsTable("unisadw.DimCourseResource")