from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("FactLearnOnlineEngagement")\
 .getOrCreate()

logDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/NewLog.json', format='json')
logDF = logDF.filter(logDF.filename != 'NEWLogBUIL 1024 _1094_SP42020.csv')
logDF = logDF.drop('_id').distinct()

logDF = logDF.withColumn('course', regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})', 2))
logDF = logDF.withColumn('StudyPeriod', regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})\s?_?(SP\d{1})', 3))
logDF = logDF.withColumn('Year', when(col('filename')=='NEWLoglogs_MATH 1077 SP3021_20210925-1114.csv', '2021')
                                .otherwise(regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})\s?_?(SP\d{1})\s?(\d{4})', 4)))
logDF = logDF.withColumn('TermCode', when(col('StudyPeriod') == 'SP1', concat(substring(col('Year'), 3, 2), lit('05')))
                                    .when(col('StudyPeriod') == 'SP3', concat(substring(col('Year'), 3, 2), lit('12')))
                                    .when(col('StudyPeriod') == 'SP4', concat(substring(col('Year'), 3, 2), lit('14')))
                                    .when(col('StudyPeriod') == 'SP6', concat(substring(col('Year'), 3, 2), lit('25'))))

logDF = logDF.withColumn('TimeFormed', to_timestamp(col('Time'), 'dd/MM/yy, HH:mm'))
logDF = logDF.withColumn('DateFormed', to_date(logDF.TimeFormed))
logDF = logDF.filter(logDF.TimeFormed.isNotNull())

logDF = logDF.withColumn('ActivityResourceType', when(trim(split(col('Event context'), ':').getItem(0)) == 'Other', col('Component'))
                                                .when(trim(split(col('Event context'), ':').getItem(0)) == 'Course', col('Component'))
                                                .otherwise(trim(split(col('Event context'), ':').getItem(0))))

logDF = logDF.withColumn('ActivityResourceName', trim(split(col('Event context'), ':').getItem(1)))
logDF = logDF.withColumn('SubjectArea', trim(split(col('course'), ' ').getItem(0)))
logDF = logDF.withColumn('CatalogNumber', trim(split(col('course'), ' ').getItem(1)))

DimStudent = spark.sql("SELECT * FROM `unisadw`.`dimstudent`")
DimDate = spark.sql("SELECT * FROM `unisadw`.`dimdate`")
DimTime = spark.sql("SELECT * FROM `unisadw`.`dimtime`")
DimActivity = spark.sql("SELECT * FROM `unisadw`.`dimactivity`")
DimResource = spark.sql("SELECT * FROM `default`.`dimcourseresource`")

logDF = logDF.withColumn('StudentID', when(col('Student ID').contains('.'), regexp_extract(col('Student ID'), r'(\w+).', 1))
                                    .otherwise(col('Student ID')))
logDF = logDF.withColumnRenamed('Event name', 'ActivityName')

logDF = logDF.join(DimDate, logDF.DateFormed == DimDate.FullDateAlternateKey, 'left')\
            .join(DimTime, [hour(logDF.TimeFormed) == DimTime.Hour,
                            minute(logDF.TimeFormed) == DimTime.Minute,
                            second(logDF.TimeFormed) == DimTime.Second], 'left')\
            .join(DimStudent, [DimStudent.StudentID == logDF.StudentID,
                                logDF.TermCode >= DimStudent.EffectiveTerm,
                                logDF.TermCode < DimStudent.ExpiryTerm], 'left')\
            .join(DimResource, ['ActivityResourceName', 'ActivityResourceType', 'SubjectArea', 'CatalogNumber', 'TermCode'], 'left')\
            .join(DimActivity, 'ActivityName', 'left')\
            .select(logDF.TimeFormed.alias('EngageTime'), logDF.StudentID, logDF.TermCode, DimDate.DateKey.alias('EngageDateSK'), DimTime.TimeSK.alias('EngageTimeSK'), 
                    DimStudent.StudentSK, DimResource.ResourceSK, DimActivity.ActivitySK)

logDF = logDF.drop('StudentID')
logDF.write.mode("append").saveAsTable("unisadw.factlearnonlineengagement")