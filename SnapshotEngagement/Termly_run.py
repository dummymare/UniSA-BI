from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("SnapshotEngagement")\
 .getOrCreate()

CourseVisitDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/coursesitevisits.json', format='json')
CourseVisitDF = CourseVisitDF.drop('_id')
CourseVisitDF = CourseVisitDF.distinct()

CourseVisitDF = CourseVisitDF.withColumnRenamed('Subject Area', 'SubjectArea')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Catalog Number', 'CatalogNumber')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Term Code', 'TermCode')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Student ID', 'StudentID')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Visit Count', 'VisitCount')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Engagement Count', 'EngagementCount')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Engagement Percentage', 'EngagementPercentage')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Current Grade', 'CurrentGrade')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Current Grade Letter', 'CurrentGradeLetter')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Official Grade', 'OfficialGrade')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Official Grade Letter', 'OfficialGradeLetter')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Last Course Login Date', 'LastCourseLoginDate')
CourseVisitDF = CourseVisitDF.withColumnRenamed('Course ID', 'CourseID')

#Load dim tables and video view
factVideo = spark.read.options(header='True', inferSchema='True', delimiter=',')\
                .csv("abfss://datalake@dus02store.dfs.core.windows.net/reOrganized_v2/NewVideo/newVideoMerged_v2.csv")
DimDate = spark.sql("SELECT * FROM `unisadw`.`dimdate`")
DimGrade = spark.sql("SELECT * FROM `unisadw`.`dimgrade`")
DimStudent = spark.sql("SELECT * FROM `unisadw`.`dimstudent`")
DimTime = spark.sql("SELECT * FROM `unisadw`.`dimtime`")
DimCourse = spark.sql("SELECT * FROM `unisadw`.`dimcourse`")

#Processing video watching summary
factVideo = factVideo.withColumnRenamed('Student ID', 'StudentID')
factVideo = factVideo.withColumnRenamed('Views and DownLoads', 'ViewsDownLoads')
factVideo = factVideo.withColumnRenamed('Minutes Delivered', 'MinutesDelivered')
factVideo = factVideo.withColumnRenamed('Average Minutes Delivered', 'AverageMinutesDelivered')

factVideo = factVideo.select('StudentID', 'ViewsDownLoads', 'MinutesDelivered', 'AverageMinutesDelivered', 'degree', 'course', 'termYear')

factVideo = factVideo.withColumn('StudyPeriod', regexp_extract(factVideo.termYear, r'SP(\d?)', 1))\
                        .withColumn('Year', when(factVideo.termYear == 'UPDATED_INFT 1024 SP4 2020 NO Assessment Submission Data', 20)
                                            .otherwise(regexp_extract(element_at(split(factVideo.termYear, '[_ ]'), -1), r'20(\d+)', 1)))

factVideo = factVideo.withColumn('TermCode', when(factVideo.StudyPeriod == '1', concat(factVideo.Year, lit('05')).cast(IntegerType()))
                                            .when(factVideo.StudyPeriod == '3', concat(factVideo.Year, lit('12')).cast(IntegerType()))
                                            .when(factVideo.StudyPeriod == '4', concat(factVideo.Year, lit('14')).cast(IntegerType()))
                                            .when(factVideo.StudyPeriod == '6', concat(factVideo.Year, lit('25')).cast(IntegerType()))
                                            .otherwise(None))

factVideo = factVideo.filter(factVideo.TermCode.isNotNull())
factVideo = factVideo.withColumn('course', when(factVideo.course == 'MENG1014', 'MENG 1014').otherwise(factVideo.course))

#Look up for surrogate keys
CourseVisitDF = CourseVisitDF.withColumn('LastLoginTime', to_timestamp(CourseVisitDF.LastCourseLoginDate, 'dd/MM/yyyy hh:mm:ss a'))
CourseVisitDF = CourseVisitDF.join(DimCourse, ['CourseID', 'TermCode'], 'left')\
                                .join(factVideo, [CourseVisitDF.StudentID == factVideo.StudentID,
                                                CourseVisitDF.TermCode == factVideo.TermCode,
                                                concat_ws(' ', CourseVisitDF.SubjectArea, CourseVisitDF.CatalogNumber.cast(StringType())) == factVideo.course], 'left')\
                                .join(DimStudent, [CourseVisitDF.StudentID == DimStudent.StudentID,
                                                CourseVisitDF.TermCode >= DimStudent.EffectiveTerm,
                                                CourseVisitDF.TermCode < DimStudent.ExpiryTerm], 'left')\
                                .join(DimDate, to_date(CourseVisitDF.LastLoginTime) == DimDate.FullDateAlternateKey, 'left')\
                                .join(DimTime, [hour(CourseVisitDF.LastLoginTime) == DimTime.Hour,
                                            minute(CourseVisitDF.LastLoginTime) == DimTime.Minute,
                                            second(CourseVisitDF.LastLoginTime) == DimTime.Second], 'left')\
                                .select(CourseVisitDF.VisitCount, CourseVisitDF.EngagementCount, CourseVisitDF.EngagementPercentage,
                                        CourseVisitDF.CurrentGrade, CourseVisitDF.CurrentGradeLetter, CourseVisitDF.OfficialGrade,
                                        CourseVisitDF.OfficialGradeLetter, CourseVisitDF.LastLoginTime, 
                                        factVideo.ViewsDownLoads, factVideo.MinutesDelivered, factVideo.AverageMinutesDelivered,
                                        DimCourse.CourseSK, DimStudent.StudentSK, DimDate.DateKey.alias('LastLoginDateSK'),
                                        DimTime.TimeSK.alias('LastLoginTimeSK'))

CourseVisitDF = CourseVisitDF.join(DimGrade, CourseVisitDF.CurrentGradeLetter == DimGrade.Grade, 'left')\
                            .select(CourseVisitDF.VisitCount, CourseVisitDF.EngagementCount, CourseVisitDF.EngagementPercentage,
                                    CourseVisitDF.CurrentGrade, CourseVisitDF.OfficialGrade,
                                    CourseVisitDF.OfficialGradeLetter, CourseVisitDF.LastLoginTime,
                                    CourseVisitDF.CourseSK, CourseVisitDF.StudentSK, 
                                    CourseVisitDF.LastLoginDateSK, CourseVisitDF.LastLoginTimeSK,
                                    CourseVisitDF.ViewsDownLoads, CourseVisitDF.MinutesDelivered, CourseVisitDF.AverageMinutesDelivered,
                                    DimGrade.GradeSK.alias('CurrentGradeSK'))

CourseVisitDF = CourseVisitDF.join(DimGrade, CourseVisitDF.OfficialGradeLetter == DimGrade.Grade, 'left')\
                            .select(CourseVisitDF.VisitCount, CourseVisitDF.EngagementCount, CourseVisitDF.EngagementPercentage,
                                    CourseVisitDF.CurrentGrade, CourseVisitDF.OfficialGrade, CourseVisitDF.LastLoginTime,
                                    CourseVisitDF.ViewsDownLoads, CourseVisitDF.MinutesDelivered, CourseVisitDF.AverageMinutesDelivered,
                                    CourseVisitDF.CourseSK, CourseVisitDF.StudentSK, 
                                    CourseVisitDF.LastLoginDateSK, CourseVisitDF.LastLoginTimeSK, CourseVisitDF.CurrentGradeSK,
                                    DimGrade.GradeSK.alias('OfficialGradeSK'))

CourseVisitDF.write.mode("append").saveAsTable("unisadw.snapshotengagement")