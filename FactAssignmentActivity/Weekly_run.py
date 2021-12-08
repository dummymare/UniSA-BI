from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("FactAssignmentActivity")\
 .getOrCreate()

logDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/NewLog.json', format='json')
logDF = logDF.filter(logDF.filename != 'NEWLogBUIL 1024 _1094_SP42020.csv')

#Remove duplicates
logDF = logDF.drop('_id').distinct()

#Fix columns
logDF = logDF.withColumn('course', regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})', 2))
logDF = logDF.withColumn('StudyPeriod', regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})\s?_?(SP\d{1})', 3))
logDF = logDF.withColumn('Year', when(col('filename')=='NEWLoglogs_MATH 1077 SP3021_20210925-1114.csv', '2021')
                                .otherwise(regexp_extract(upper(logDF.filename), r'NEWLOG(LOGS)?_?([A-Z]{4}\s\d{4})\s?_?(SP\d{1})\s?(\d{4})', 4)))
logDF = logDF.withColumn('TermCode', when(col('StudyPeriod') == 'SP1', concat(substring(col('Year'), 3, 2), lit('05')))
                                    .when(col('StudyPeriod') == 'SP3', concat(substring(col('Year'), 3, 2), lit('12')))
                                    .when(col('StudyPeriod') == 'SP4', concat(substring(col('Year'), 3, 2), lit('14')))
                                    .when(col('StudyPeriod') == 'SP6', concat(substring(col('Year'), 3, 2), lit('25'))))
logDF = logDF.withColumn('TermCode', col('TermCode').cast(IntegerType()))

logDF = logDF.withColumn('TimeFormed', to_timestamp(col('Time'), 'dd/MM/yy, HH:mm'))
logDF = logDF.withColumn('DateFormed', to_date(logDF.TimeFormed))
logDF = logDF.filter(logDF.TimeFormed.isNotNull())

#Load dimension tables
DimStudent = spark.sql("SELECT * FROM `unisadw`.`dimstudent`")
DimDate = spark.sql("SELECT * FROM `unisadw`.`dimdate`")
DimTime = spark.sql("SELECT * FROM `unisadw`.`dimtime`")
DimAssignment = spark.sql("SELECT * FROM `unisadw`.`dimassignment`")
DimActivity = spark.sql("SELECT * FROM `unisadw`.`dimactivity`")

#Look up for activities related to assignments
logDF = logDF.filter((col('Component') == 'Assignment') | (split(col('Event context'), ':').getItem(0) == 'Assignment'))
logDF = logDF.withColumn('AssessmentName', trim(split(col('Event context'), ':').getItem(1)))
logDF = logDF.withColumnRenamed('Event name', 'ActivityName')

#Fix Student ID
logDF = logDF.withColumn('StudentID', when(col('Student ID').contains('.'), regexp_extract(col('Student ID'), r'(\w+).', 1))
                                    .otherwise(col('Student ID')))

#Look up for surrogate keys
logDF = logDF.join(DimDate, logDF.DateFormed == DimDate.FullDateAlternateKey, 'left')\
            .join(DimTime, [hour(logDF.TimeFormed) == DimTime.Hour,
                            minute(logDF.TimeFormed) == DimTime.Minute,
                            second(logDF.TimeFormed) == DimTime.Second], 'left')\
            .join(DimStudent, [DimStudent.StudentID == logDF.StudentID,
                                logDF.TermCode >= DimStudent.EffectiveTerm,
                                logDF.TermCode < DimStudent.ExpiryTerm], 'left')\
            .join(DimAssignment, [logDF.AssessmentName == DimAssignment.AssessmentName, 
                                    logDF.TermCode == DimAssignment.TermCode,
                                    logDF.course == concat_ws(' ', DimAssignment.SubjectArea, DimAssignment.CatalogNumber)], 'left')\
            .join(DimActivity, 'ActivityName', 'left')\
            .select(logDF.TimeFormed.alias('EngageTime'), logDF.StudentID, DimDate.DateKey.alias('EngageDateSK'), DimTime.TimeSK.alias('EngageTimeSK'), 
                                DimStudent.StudentSK, DimAssignment.AssignmentSK, DimActivity.ActivitySK)

#Write tables
logDF.write.mode("append").saveAsTable("unisadw.factassignmentengagement")