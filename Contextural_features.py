from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce

spark = SparkSession\
 .builder\
 .appName("DataPrep")\
 .getOrCreate()

latesubDF = spark.sql('select * from unisadw.latesubinfo')
nosubDF = spark.sql('select * from unisadw.nosubinfo')
latesubheadDF = spark.sql('select * from unisadw.latesubheadinfo')
nosubheadDF = spark.sql('select * from unisadw.nosubheadinfo')
engagementDF = spark.sql('select * from unisadw.engagementinfo')
engagementheadDF = spark.sql('select * from unisadw.engagementheadinfo')
enrolmentDF = spark.sql('select * from unisadw.enrolmentinfo')
studentDF = spark.sql('select * from unisadw.studentinfo')

latesubCourseDF = spark.sql('select * from unisadw.latesubheadcourse')
nosubCourseDF = spark.sql('select * from unisadw.nosubheadcourse')
engagementCourseDF = spark.sql('select * from unisadw.engagementheadcourse')

termCodes = [1825, 1905, 1912, 1914, 1925, 2005, 2012, 2014, 2025, 2105, 2112]

nProgramList = []
pastlatesubList = []
pastnosubList = []
pastengagementList = []
pastenrolmentList = []

TotalLateSubList = []
TotalNoSubList = []
TotalEngagementList = []
TotalEnrolmentList = []

for term in termCodes:
    if term == 1825:
        continue
    
    #Last Enrolled Term
    lastTermDF = enrolmentDF.filter(col('TermCode')<term).groupBy('StudentID').agg(max('TermCode').alias('TermCode'))

    #Changed field of study
    nProgramDF = enrolmentDF\
                    .filter(col('TermCode')<=term)\
                    .groupBy('StudentID')\
                    .agg(countDistinct('MajorProgramCode').alias('nPrograms'))\
                    .withColumn('TermCode', lit(term))

    #Late submission history
    pastlatesubDF = latesubDF\
                        .join(lastTermDF, ['StudentID', 'TermCode'])\
                        .select(latesubDF.StudentID, 'NumberOfLateSubmissions', 'TotalLateHours')\
                        .withColumn('TermCode', lit(term))

    TotalLateSubDF = latesubDF\
                        .filter(col('TermCode')<term)\
                        .groupBy('StudentID')\
                        .agg(sum('NumberOfLateSubmissions').alias('TotalLateSubmissions'), sum('TotalLateHours').alias('TotalLateHours'))\
                        .withColumn('TermCode', lit(term))
    
    #Invalid Submission history
    pastnosubDF = nosubDF\
                    .join(lastTermDF, ['StudentID', 'TermCode'])\
                    .select(nosubDF.StudentID, 'NumberOfInvalidSubmission')\
                    .withColumn('TermCode', lit(term))

    TotalNoSubDF = nosubDF\
                    .filter(col('TermCode')<term)\
                    .groupBy('StudentID')\
                    .agg(sum('NumberOfInvalidSubmission').alias('TotalInvalidSubmission'))\
                    .withColumn('TermCode', lit(term))

    #Engagement history
    pastengagementDF = engagementDF\
                            .join(lastTermDF, ['StudentID', 'TermCode'])\
                            .select(engagementDF.StudentID, 'ActivityResourceType', 'EngagementCount')\
                            .withColumn('TermCode', lit(term))

    TotalEngagementDF = engagementDF\
                            .filter(col('TermCode')<term)\
                            .groupBy(['StudentID', 'ActivityResourceType'])\
                            .agg(sum('EngagementCount').alias('TotalEngagementCount'))\
                            .withColumn('TermCode', lit(term))

    #Enrolment history
    pastenrolmentDF = enrolmentDF\
                        .join(lastTermDF, ['StudentID', 'TermCode'])\
                        .select(enrolmentDF.StudentID, 'NumberOfEnrolments', 'GradeAverage', 'GPA',
                                'TotalVideoView', 'TotalMinutesDelivered', 'TotalEngagementCount')\
                        .withColumn('TermCode', lit(term))

    TotalEnrolmentDF = enrolmentDF\
                            .filter(col('TermCode')<term)\
                            .groupBy('StudentID')\
                            .agg(sum('NumberOfEnrolments').alias('TotalEnrolments'),
                                (sum(col('GPA')*col('NumberOfEnrolments'))/sum('NumberOfEnrolments')).alias('GPA'),
                                sum('TotalVideoView').alias('TotalVideoView'), sum('TotalMinutesDelivered').alias('TotalMinutesDelivered'),
                                sum('TotalEngagementCount').alias('TotalEngagementCount'))\
                            .withColumn('TermCode', lit(term))


    nProgramList.append(nProgramDF)

    pastlatesubList.append(pastengagementDF)
    pastnosubList.append(pastnosubDF)
    pastengagementList.append(pastengagementDF)
    pastenrolmentList.append(pastenrolmentDF)

    TotalLateSubList.append(TotalLateSubDF)
    TotalNoSubList.append(TotalNoSubDF)
    TotalEngagementList.append(TotalEngagementDF)
    TotalEnrolmentList.append(TotalEngagementDF)

#Merge
nProgramDF = reduce(lambda x, y: x.union(y), nProgramList)

pastlatesubDF = reduce(lambda x, y: x.union(y), pastlatesubList)
pastnosubDF = reduce(lambda x, y: x.union(y), pastnosubList)
pastengagementDF = reduce(lambda x, y: x.union(y), pastengagementList)
pastenrolmentDF = reduce(lambda x, y: x.union(y), pastenrolmentList)

TotalLateSubDF = reduce(lambda x, y: x.union(y), TotalLateSubList)
TotalNoSubDF = reduce(lambda x, y: x.union(y), TotalNoSubList)
TotalEngagementDF = reduce(lambda x, y: x.union(y), TotalEngagementList)
TotalEnrolmentDF = reduce(lambda x, y: x.union(y), TotalEnrolmentList)

#Pivotting for engagement activities
pastengagementDF = pastengagementDF\
                        .groupBy(['StudentID', 'TermCode'])\
                        .pivot('ActivityResourceType')\
                        .sum('EngagementCount')

TotalEngagementDF = TotalEngagementDF\
                        .groupBy(['StudentID', 'TermCode'])\
                        .pivot('ActivityResourceType')\
                        .sum('TotalEngagementCount')

engagementheadDF = engagementheadDF\
                        .groupBy(['StudentID', 'TermCode'])\
                        .pivot('ActivityResourceType')\
                        .sum('NumberOfEngagements')

engagementCourseDF = engagementCourseDF\
                        .groupBy(['StudentID', 'TermCode', 'CourseID'])\
                        .pivot('ActivityResourceType')\
                        .sum('NumberOfEngagements')

#Join features
studentDF.join(nProgramDF, ['StudentID', 'TermCode'], 'left')\
         .join(pastlatesubDF, ['StudentID', 'TermCode'], 'left')\
         .join(pastnosubDF, ['StudentID', 'TermCode'], 'left')\
         .join(pastengagementDF, ['StudentID', 'TermCode'], 'left')\
         .join(pastenrolmentDF, ['StudentID', 'TermCode'], 'left')\
         .join(TotalLateSubDF, ['StudentID', 'TermCode'], 'left')\
         .join(TotalNoSubDF, ['StudentID', 'TermCode'], 'left')\
         .join(TotalEngagementDF, ['StudentID', 'TermCode'], 'left')\
         .join(TotalEnrolmentDF, ['StudentID', 'TermCode'], 'left')\
         .join(latesubheadDF, ['StudentID', 'TermCode'], 'left')\
         .join(nosubheadDF, ['StudentID', 'TermCode'], 'left')\
         .join(engagementheadDF, ['StudentID', 'TermCode'], 'left')\
         .join(enrolmentDF, ['StudentID', 'TermCode'], 'left')\
         .join(latesubCourseDF, ['StudentID', 'TermCode', 'CourseID'], 'left')\
         .join(nosubCourseDF, ['StudentID', 'TermCode', 'CourseID'], 'left')\
         .join(engagementCourseDF, ['StudentID', 'TermCode', 'CourseID'], 'left')\
         .select('studentDF.*', col('nPrograms'), pastlatesubDF.NumberOfLateSubmissions, pastlatesubDF.TotalLateHours, 
                (pastlatesubDF.TotalLateHours/pastlatesubDF.NumberOfLateSubmissions).alias('AvgLateHours'),
                pastnosubDF.NumberOfInvalidSubmission, 'pastengagementDF.*', pastenrolmentDF.NumberOfEnrolments.alias('nPastEnrolments'),
                pastenrolmentDF.GradeAverage, pastenrolmentDF.GPA, pastenrolmentDF.TotalVideoView, pastenrolmentDF.TotalMinutesDelivered,
                pastenrolmentDF.TotalEngagementCount, 'engagementheadDF.*', enrolmentDF.NumberOfEnrolments, enrolmentDF.enrolledCourses,
                latesubheadDF.NumberOfLateSubmissions, latesubheadDF.avg_latehours, nosubheadDF.NumberOfInvalidSubmission,
                latesubCourseDF.NumberOfLateSubmissions, latesubCourseDF.avg_latehours, nosubCourseDF.NumberOfInvalidSubmission, 'engagementCourseDF.*',
                TotalLateSubDF.TotalLateSubmissions, TotalLateSubDF.TotalLateHours, TotalNoSubDF.TotalInvalidSubmission, 'TotalEngagementDF*',
                TotalEnrolmentDF.TotalEnrolments, TotalEnrolmentDF.GPA, TotalEnrolmentDF.TotalVideoView, 
                TotalEnrolmentDF.TotalMinutesDelivered, TotalEnrolmentDF.TotalEngagementCount, 
                (TotalLateSubDF.TotalLateHours/TotalLateSubDF.TotalLateSubmissions).alias('OverallAvgLateHours'))\
         .write.save('abfss://datalake@dus02store.dfs.core.windows.net/DataPrep/ContextFeatures/', format='parquet', mode='overwrite')