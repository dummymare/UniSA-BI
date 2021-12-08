from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("SnapshotLO")\
 .getOrCreate()

learnOnlineDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/learnonline.json', format='json')

#Remove duplicates
learnOnlineDF = learnOnlineDF.drop('_id')
learnOnlineDF = learnOnlineDF.distinct()

#load dim tables
DimCourseResource = spark.sql("SELECT * FROM `unisadw`.`dimcourseresource`")
DimStudent = spark.sql("SELECT * FROM `unisadw`.`dimstudent`")

#Look up for surrogate keys
learnOnlineDF = learnOnlineDF.join(DimCourseResource, ['Year', 'ActivityResourceName', 'ActivityResourceType', 'AssessmentMethod', 'CatalogNumber',
                                'CourseID', 'CourseName', 'SubjectArea', 'TermCode'], 'left')\
                            .join(DimStudent, [learnOnlineDF.StudentID == DimStudent.StudentID,
                                                learnOnlineDF.TermCode >= DimStudent.EffectiveTerm,
                                                learnOnlineDF.TermCode < DimStudent.ExpiryTerm], 'left')\
                            .select(learnOnlineDF.NumberOfViews, learnOnlineDF.NumberOfContributes, learnOnlineDF.TotalContributions,
                                    learnOnlineDF.TotalViews, learnOnlineDF.PercentTotalContributions, learnOnlineDF.PercentTotalViews,
                                    DimCourseResource.ResourceSK, DimStudent.StudentSK)

learnOnlineDF = learnOnlineDF.join(DimStudent, [learnOnlineDF.StudentID == DimStudent.StudentID,
                                                learnOnlineDF.TermCode >= DimStudent.EffectiveTerm,
                                                learnOnlineDF.TermCode < DimStudent.ExpiryTerm], 'left')\
                            .select(learnOnlineDF.NumberOfViews, learnOnlineDF.NumberOfContributes, learnOnlineDF.TotalContributions,
                                    learnOnlineDF.TotalViews, learnOnlineDF.PercentTotalContributions, learnOnlineDF.PercentTotalViews,
                                    DimStudent.StudentSK)

learnOnlineDF.write.mode("append").saveAsTable("unisadw.snapshotlearnonline")