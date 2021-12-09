from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("SnapshotSubmission")\
 .getOrCreate()

#Function to change year
def change_year_(date, Year):
    return datetime(date).replace(year=Year)

change_year = udf(change_year_, TimestampType())

#Load from mongo & remove duplicates
subvgradeDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/newsubmissionvgrades.json', format='json')

subvgradeDF = subvgradeDF.drop('_id')
subvgradeDF = subvgradeDF.distinct()

#Load dim tables
DimAssignment = spark.sql("SELECT * FROM `unisadw`.`dimassignment`")
DimDate = spark.sql("SELECT * FROM `unisadw`.`dimdate`")
DimGrade = spark.sql("SELECT * FROM `unisadw`.`dimgrade`")
DimStudent = spark.sql("SELECT * FROM `unisadw`.`dimstudent`")
DimTime = spark.sql("SELECT * FROM `unisadw`.`dimtime`")

#Rename columns
subvgradeDF = subvgradeDF.withColumnRenamed('Assessment Name', 'AssessmentName')
subvgradeDF = subvgradeDF.withColumnRenamed('Course ID', 'CourseID')
subvgradeDF = subvgradeDF.withColumnRenamed('Subject Area', 'SubjectArea')
subvgradeDF = subvgradeDF.withColumnRenamed('Catalog Number', 'CatalogNumber')
subvgradeDF = subvgradeDF.withColumnRenamed('Course Name', 'CourseName')
subvgradeDF = subvgradeDF.withColumnRenamed('Term Code', 'TermCode')
subvgradeDF = subvgradeDF.withColumnRenamed('Student ID', 'StudentID')
subvgradeDF = subvgradeDF.withColumnRenamed('Grade Numeric', 'GradeNumeric')

#Fix Time
subvgradeDF = subvgradeDF.withColumn('IndividualDueTime', to_timestamp(col("Student's Due Date"), 'dd/MM/yyyy hh:mm:ss a'))\
                        .withColumn('IndividualDueTimeAlter', to_timestamp(col("Student's Due Date"), 'dd/MM/yyyy HH:mm'))
subvgradeDF = subvgradeDF.withColumn('IndividualDueTime', when(isnull(subvgradeDF.IndividualDueTime), subvgradeDF.IndividualDueTimeAlter)
                                                            .otherwise(subvgradeDF.IndividualDueTime))
subvgradeDF = subvgradeDF.withColumn('IndividualDueTime', when(year(subvgradeDF.IndividualDueTime) == 20, change_year(subvgradeDF.IndividualDueTime, lit(2020)))
                                                            .otherwise(subvgradeDF.IndividualDueTime))

subvgradeDF = subvgradeDF.withColumn('LastSubmissionTime', to_timestamp(col("Last Submission Time"), 'dd/MM/yyyy hh:mm:ss a'))\
                        .withColumn('LastSubmissionTimeAlter', to_timestamp(col("Last Submission Time"), 'dd/MM/yyyy HH:mm'))
subvgradeDF = subvgradeDF.withColumn('LastSubmissionTime', when(isnull(subvgradeDF.LastSubmissionTime), subvgradeDF.LastSubmissionTimeAlter)
                                                            .otherwise(subvgradeDF.LastSubmissionTime))
subvgradeDF = subvgradeDF.withColumn('LastSubmissionTime', when((year(subvgradeDF.LastSubmissionTime) == 20) & subvgradeDF.LastSubmissionTime.isNotNull(), 
                                                                change_year(subvgradeDF.LastSubmissionTime, lit(2020)))
                                                            .otherwise(subvgradeDF.LastSubmissionTime))

#Look up for Surrogate keys
subvgradeDF = subvgradeDF.join(DimAssignment, ['AssessmentName', 'CourseID', 'TermCode'], 'left')\
                        .join(DimStudent, [subvgradeDF.StudentID == DimStudent.StudentID, 
                                                subvgradeDF.TermCode >= DimStudent.EffectiveTerm,
                                                subvgradeDF.TermCode < DimStudent.ExpiryTerm], 'left')\
                        .join(DimGrade, 'Grade', 'left')\
                        .select(subvgradeDF.GradeNumeric, subvgradeDF.IndividualDueTime, subvgradeDF.LastSubmissionTime,
                                DimAssignment.AssignmentSK, DimStudent.StudentSK, DimGrade.GradeSK)

subvgradeDF = subvgradeDF.join(DimDate, to_date(subvgradeDF.IndividualDueTime) == DimDate.FullDateAlternateKey, 'left')\
                        .join(DimTime, [hour(subvgradeDF.IndividualDueTime) == DimTime.Hour,
                                        minute(subvgradeDF.IndividualDueTime) == DimTime.Minute,
                                        second(subvgradeDF.IndividualDueTime) == DimTime.Second], 'left')\
                        .select(subvgradeDF.GradeNumeric, subvgradeDF.IndividualDueTime, subvgradeDF.LastSubmissionTime,
                                subvgradeDF.AssignmentSK, subvgradeDF.StudentSK, subvgradeDF.GradeSK,
                                DimDate.DateKey.alias('IndividualDueDateSK'), DimTime.TimeSK.alias('IndividualDueTimeSK'))

subvgradeDF = subvgradeDF.join(DimDate, to_date(subvgradeDF.LastSubmissionTime) == DimDate.FullDateAlternateKey, 'left')\
                        .join(DimTime, [hour(subvgradeDF.LastSubmissionTime) == DimTime.Hour,
                                        minute(subvgradeDF.LastSubmissionTime) == DimTime.Minute,
                                        second(subvgradeDF.LastSubmissionTime) == DimTime.Second], 'left')\
                        .select(subvgradeDF.GradeNumeric, subvgradeDF.IndividualDueTime, subvgradeDF.LastSubmissionTime,
                                subvgradeDF.AssignmentSK, subvgradeDF.StudentSK, subvgradeDF.GradeSK,
                                subvgradeDF.IndividualDueDateSK, subvgradeDF.IndividualDueTimeSK,
                                DimDate.DateKey.alias('LastSubmissionDateSK'), DimTime.TimeSK.alias('LastSubmissionTimeSK'))

subvgradeDF.write.mode("append").saveAsTable("unisadw.snapshotfactsubmission")