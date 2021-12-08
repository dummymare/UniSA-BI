from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.orderBy("CourseID")

spark = SparkSession\
 .builder\
 .appName("DimAssignmentInit")\
 .getOrCreate()

def change_year_(date, Year):
    return date.replace(year=Year) if date else date

change_year = udf(change_year_, TimestampType())

#Load Data
latesubDF = spark.sql("SELECT * FROM `default`.`latesubmerged`")
subvgradeDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/newsubmissionvgrades.json', format='json')

DateDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/local/QID1109_20210913_202220_1.parq', format='parquet')
TimeDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/local/DimTime.parq', format='parquet')

#Rename Columns
subvgradeDF = subvgradeDF.withColumnRenamed('Assessment Name', 'AssessmentName')\
                         .withColumnRenamed('Course ID', 'CourseID')\
                         .withColumnRenamed('Subject Area', 'SubjectArea')\
                         .withColumnRenamed('Catalog Number', 'CatalogNumber')\
                         .withColumnRenamed('Course Name', 'CourseName')\
                         .withColumnRenamed('Term Code', 'TermCode')

#Transform Student's due dates as supplimentory Initial due dates
subvgradeDF = subvgradeDF.withColumn('StudentDueTime', to_timestamp(col("Student's Due Date"), "dd/MM/yyyy hh:mm:ss a"))\
                        .withColumn("StudentDueTimeAlter", to_timestamp(col("Student's Due Date"), "dd/MM/yyyy HH:mm"))
subvgradeDF = subvgradeDF.withColumn('StudentDueTime', when(isnull(subvgradeDF.StudentDueTime), subvgradeDF.StudentDueTimeAlter)
                                                        .otherwise(subvgradeDF.StudentDueTime))

#Transform Initial & Global extension due dates
latesubDF = latesubDF.withColumn('InitialDueDateFormed', to_timestamp(latesubDF.InitialDueDate, "dd/MM/yyyy hh:mm:ss a"))\
                    .withColumn('InitialDueDateFormedAlter1', to_timestamp(latesubDF.InitialDueDate, 'dd/MM/yyyy HH:mm'))\
                    .withColumn('InitialDueDateFormedAlter2', to_timestamp(latesubDF.InitialDueDate, 'dd/MM/yy HH:mm'))\
                    .withColumn('GlobalExtensionDueDateFormed', to_timestamp(latesubDF.GlobalExtensionDueDate, "dd/MM/yyyy hh:mm:ss a"))\
                    .withColumn('GlobalExtensionDueDateFormedAlter1', to_timestamp(latesubDF.GlobalExtensionDueDate, "dd/MM/yyyy HH:mm"))\
                    .withColumn('GlobalExtensionDueDateFormedAlter2', to_timestamp(latesubDF.GlobalExtensionDueDate, "dd/MM/yy HH:mm"))

latesubDF = latesubDF.withColumn('InitialDueDateFormed', 
                                when(isnull(latesubDF.InitialDueDateFormed) & isnull(latesubDF.InitialDueDateFormedAlter1), 
                                    latesubDF.InitialDueDateFormedAlter2)
                                .when(isnull(latesubDF.InitialDueDateFormed) & latesubDF.InitialDueDateFormedAlter1.isNotNull(),
                                    latesubDF.InitialDueDateFormedAlter1)
                                .otherwise(latesubDF.InitialDueDateFormed))

latesubDF = latesubDF.withColumn('GlobalExtensionDueDateFormed',
                                when(isnull(latesubDF.GlobalExtensionDueDateFormed) & isnull(latesubDF.GlobalExtensionDueDateFormedAlter1), 
                                    latesubDF.GlobalExtensionDueDateFormedAlter2)
                                .when(isnull(latesubDF.GlobalExtensionDueDateFormed) & latesubDF.GlobalExtensionDueDateFormedAlter1.isNotNull(),
                                    latesubDF.GlobalExtensionDueDateFormedAlter1)
                                .otherwise(latesubDF.GlobalExtensionDueDateFormed))

#Remove duplicates
subvgradeDF = subvgradeDF.groupBy(['AssessmentName', 'CourseID', 'SubjectArea', 'CatalogNumber', 'CourseName', 'TermCode', 'Year'])\
                            .agg(min('StudentDueTime').alias('DueTime'))

latesubDF = latesubDF.groupBy(['AssessmentName', 'InitialDueDateFormed', 'GlobalExtensionDueDateFormed', 'CourseID', 'SubjectArea', 'CatalogNumber', 'CourseName', 
                                'TermCode', 'Year']).count()

#Join LateSub and subvgradeDF
subvgradeDF = subvgradeDF.join(latesubDF, ['AssessmentName', 'CourseID', 'SubjectArea', 'CatalogNumber', 'CourseName', 'TermCode'], 'left')\
                        .select(subvgradeDF.AssessmentName, subvgradeDF.CourseID, subvgradeDF.SubjectArea, subvgradeDF.CatalogNumber,
                                subvgradeDF.CourseName, subvgradeDF.TermCode, subvgradeDF.Year, latesubDF.InitialDueDateFormed,
                                latesubDF.GlobalExtensionDueDateFormed, subvgradeDF.DueTime)

subvgradeDF = subvgradeDF.withColumn('InitialDueDateFormed', when(isnull(subvgradeDF.InitialDueDateFormed), subvgradeDF.DueTime)
                                                                .otherwise(subvgradeDF.InitialDueDateFormed))

subvgradeDF = subvgradeDF.withColumn('InitialDueDateFormed', when(year(subvgradeDF.InitialDueDateFormed) == 20, 
                                                                    change_year(subvgradeDF.InitialDueDateFormed, lit(2020)))
                                                                .otherwise(subvgradeDF.InitialDueDateFormed))
subvgradeDF = subvgradeDF.withColumn('GlobalExtensionDueDateFormed', when(year(subvgradeDF.GlobalExtensionDueDateFormed) == 20, 
                                                                    change_year(subvgradeDF.GlobalExtensionDueDateFormed, lit(2020)))
                                                                .otherwise(subvgradeDF.GlobalExtensionDueDateFormed))

#Lookup for DateSK
subvgradeDF = subvgradeDF.join(DateDF, to_date(subvgradeDF.InitialDueDateFormed) == DateDF.FullDateAlternateKey, 'left')\
                .select(subvgradeDF.AssessmentName, subvgradeDF.CourseID, subvgradeDF.SubjectArea, subvgradeDF.CatalogNumber,
                    subvgradeDF.CourseName, subvgradeDF.TermCode, subvgradeDF.Year, subvgradeDF.InitialDueDateFormed,
                    subvgradeDF.GlobalExtensionDueDateFormed, DateDF.DateKey.alias('InitialDueDateSK'))

subvgradeDF = subvgradeDF.join(DateDF, to_date(subvgradeDF.GlobalExtensionDueDateFormed) == DateDF.FullDateAlternateKey, 'left')\
                .select(subvgradeDF.AssessmentName, subvgradeDF.CourseID, subvgradeDF.SubjectArea, subvgradeDF.CatalogNumber,
                    subvgradeDF.CourseName, subvgradeDF.TermCode, subvgradeDF.Year, subvgradeDF.InitialDueDateFormed,
                    subvgradeDF.GlobalExtensionDueDateFormed, subvgradeDF.InitialDueDateSK, DateDF.DateKey.alias('GlobalExtensionDueDateSK'))

#lookup for TimeSK
subvgradeDF = subvgradeDF.join(TimeDF, [hour(subvgradeDF.InitialDueDateFormed) == TimeDF.Hour,
                                        minute(subvgradeDF.InitialDueDateFormed) == TimeDF.Minute,
                                        second(subvgradeDF.InitialDueDateFormed) == TimeDF.Second], 'left')\
                            .select(subvgradeDF.AssessmentName, subvgradeDF.CourseID, subvgradeDF.SubjectArea, subvgradeDF.CatalogNumber,
                                    subvgradeDF.CourseName, subvgradeDF.TermCode, subvgradeDF.Year, subvgradeDF.InitialDueDateFormed,
                                    subvgradeDF.GlobalExtensionDueDateFormed, subvgradeDF.InitialDueDateSK, 
                                    subvgradeDF.GlobalExtensionDueDateSK, TimeDF.TimeSK.alias('InitialDueTimeSK'))

subvgradeDF = subvgradeDF.join(TimeDF, [hour(subvgradeDF.GlobalExtensionDueDateFormed) == TimeDF.Hour,
                                        minute(subvgradeDF.GlobalExtensionDueDateFormed) == TimeDF.Minute,
                                        second(subvgradeDF.GlobalExtensionDueDateFormed) == TimeDF.Second], 'left')\
                            .select(subvgradeDF.AssessmentName, subvgradeDF.CourseID, subvgradeDF.SubjectArea, subvgradeDF.CatalogNumber,
                                    subvgradeDF.CourseName, subvgradeDF.TermCode, subvgradeDF.Year, subvgradeDF.InitialDueDateFormed.alias('InitialDueTime'),
                                    subvgradeDF.GlobalExtensionDueDateFormed.alias('GlobalExtensionDueTime'), subvgradeDF.InitialDueDateSK, subvgradeDF.GlobalExtensionDueDateSK, 
                                    subvgradeDF.InitialDueTimeSK, TimeDF.TimeSK.alias('GlobalextensionDueTimeSK'))

#Save Table
subvgradeDF = subvgradeDF.withColumn('AssignmentSK', row_number().over(w))
subvgradeDF.write.mode("overwrite").saveAsTable("unisadw.DimAssignment")