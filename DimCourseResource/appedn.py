from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
w = Window.orderBy("ActivityResourceName")

spark = SparkSession\
 .builder\
 .appName("DimCourseResourceAppend")\
 .getOrCreate()

#Load origin 
DimResource = spark.sql("SELECT * FROM `unisadw`.`dimcourseresource`")
logDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/NewLog.json', format='json')
DimCourse = spark.sql("SELECT * FROM `unisadw`.`dimcourse`")

#Extract information from log line
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

logDF = logDF.groupBy(['ActivityResourceName', 'ActivityResourceType', 'course', 'TermCode', 'Year']).count()\
            .withColumn('SubjectArea', trim(split(col('course'), ' ').getItem(0)))\
            .withColumn('CatalogNumber', trim(split(col('course'), ' ').getItem(1)))

#Find additional items from log
logDF = logDF.select('ActivityResourceName', 'ActivityResourceType', 'SubjectArea', 'CatalogNumber', 'TermCode', 'Year')\
            .exceptAll(DimResource.select('ActivityResourceName', 'ActivityResourceType', 'SubjectArea', 'CatalogNumber', 'TermCode', 'Year'))

#Look up for CourseSk
logDF = logDF.join(DimCourse, ['SubjectArea', 'CatalogNumber', 'TermCode'], 'left')\
            .select('ActivityResourceName', 'ActivityResourceType', 'SubjectArea', 'CatalogNumber', 'TermCode', logDF.Year, 
                    DimCourse.CourseSK, DimCourse.CourseID, DimCourse.CourseName, DimCourse.STEMfield)\
            .withColumn('AssessmentMethod', lit(None))

#Append
DimResource = DimResource.select('CourseID', 'TermCode', 'Year', 'ActivityResourceName', 'ActivityResourceType', 
                                'AssessmentMethod', 'CatalogNumber', 'CourseName', 'SubjectArea', 'STEMfield', 'CourseSK')\
                        .union(logDF.select('CourseID', 'TermCode', 'Year', 'ActivityResourceName', 'ActivityResourceType', 
                                            'AssessmentMethod', 'CatalogNumber', 'CourseName', 'SubjectArea', 'STEMfield', 'CourseSK'))\
                        .withColumn('ResourceSK', row_number().over(w))

#Nominate STEM degress
DimResource = DimResource.withColumn('STEMfield', when(DimResource.SubjectArea == 'BUIL', lit('Construction Management'))
                                            .when(DimResource.SubjectArea == 'CIVE', lit('Construction Management'))
                                            .when(DimResource.SubjectArea == 'INFS', lit('Information Technology & Data Analysis'))
                                            .when(DimResource.SubjectArea == 'INFT', lit('Information Technology & Data Analysis'))
                                            .when(DimResource.SubjectArea == 'EEET', lit('Engineering'))
                                            .when(DimResource.SubjectArea == 'ENGG', lit('Engineering'))
                                            .when(DimResource.SubjectArea == 'MENG', lit('Engineering'))
                                            .when(DimResource.SubjectArea == 'RENG', lit('Engineering'))
                                            .when(DimResource.SubjectArea == 'PHYS', lit('Engineering'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'COMP1047', lit('Engineering'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'MATH1076', lit('Engineering'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'MATH1077', lit('Engineering'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'MATH1079', lit('Engineering'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'MATH1075', lit('Information Technology & Data Analysis'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'MATH2032', lit('Information Technology & Data Analysis'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'COMP1043', lit('Information Technology & Data Analysis'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'COMP1044', lit('Information Technology & Data Analysis'))
                                            .when(concat(DimResource.SubjectArea, DimResource.CatalogNumber.cast(StringType())) == 'COMP2033', lit('Information Technology & Data Analysis')))

#Write Table
DimResource.write.mode("overwrite").saveAsTable("default.dimcourseresource")