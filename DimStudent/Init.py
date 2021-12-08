from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd

w = Window.orderBy("StudentID")

spark = SparkSession\
 .builder\
 .appName("DimStudentInit")\
 .getOrCreate()


StudentDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/fromAtlas/newstudentlistv2.json', format='json')
DateDF = spark.read.load('abfss://datalake@dus02store.dfs.core.windows.net/local/QID1109_20210913_202220_1.parq', format='parquet')

#Reject Invalid rows
StudentDF = StudentDF.filter(StudentDF.StudentID.isNotNull())

#Fix ProgramCode
StudentDF = StudentDF.withColumn('ProgramCode', when(StudentDF.ProgramDesc == 'Associate Degree in Engineering', 'XTEN')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Business (Management)', 'XBBG')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Commerce (Accounting)', 'XBCA')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Information Technology', 'XBIT')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Information Technology and Data Analytics', 'XBCP')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Psychology', 'XBPY')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Social Work', 'MBSW')
                                                .when(StudentDF.ProgramDesc == 'Bachelor of Software Engineering (Honours)', 'LHSG')
                                                .when(StudentDF.ProgramDesc == 'OUA Short Courses UGRD', 'OUA Short Courses UGRD')
                                                .otherwise(StudentDF.ProgramCode))

#Fix Student Country
StudentDF = StudentDF.withColumn('StudentCountryDesc', when(isnull(StudentDF.StudentCountryDesc) & (StudentDF.InternationalStudentFlag == 'N'), 'Australia')
                                                        .when((StudentDF.StudentCountryDesc == '') & (StudentDF.InternationalStudentFlag == 'N'), 'Australia')
                                                        .when((StudentDF.StudentCountryDesc == 'NaN') & (StudentDF.InternationalStudentFlag == 'N'), 'Australia')
                                                        .otherwise(StudentDF.StudentCountryDesc))

#Fix Postcode Type
StudentDF =StudentDF.withColumn('StudentAddressPostcode', StudentDF.StudentAddressPostcode.cast(IntegerType()))

#Prepare Year TermCode
StudentDF = StudentDF.withColumn('StudyPeriod', regexp_extract(StudentDF.termYear, r'SP(\d?)', 1))\
                        .withColumn('Year', when(StudentDF.termYear == 'UPDATED_INFT 1024 SP4 2020 NO Assessment Submission Data', 20)
                                            .otherwise(regexp_extract(element_at(split(StudentDF.termYear, '[_ ]'), -1), r'20(\d+)', 1)))

StudentDF = StudentDF.withColumn('TermCode', when(StudentDF.StudyPeriod == '1', concat(StudentDF.Year, lit('05')).cast(IntegerType()))
                                            .when(StudentDF.StudyPeriod == '3', concat(StudentDF.Year, lit('12')).cast(IntegerType()))
                                            .when(StudentDF.StudyPeriod == '4', concat(StudentDF.Year, lit('14')).cast(IntegerType()))
                                            .when(StudentDF.StudyPeriod == '6', concat(StudentDF.Year, lit('25')).cast(IntegerType()))
                                            .otherwise(None))

#Prepare Birth Date
StudentDF = StudentDF.withColumn('BirthDate', to_date(StudentDF.StudentDOB, "dd/MM/yy"))

#Remove Duplicates
StudentDF = StudentDF.groupBy(['AcademicLoadDesc', 'GenderCode', 'InternationalStudentFlag', 
                                'ProgramCode', 'StudentAddressCityCode', 'StudentAddressPostcode', 'StudentAddressStateCode', 
                                'StudentCountryDesc', 'BirthDate', 'StudentID'])\
                        .agg(collect_set('TermCode').alias('TermCodes'), collect_set('ProgramDesc').alias('ProgramDescs'))

#Look up for BirthDate SK
StudentDF = StudentDF.join(DateDF, StudentDF.BirthDate == DateDF.FullDateAlternateKey, 'left')\
                    .select(StudentDF.AcademicLoadDesc, StudentDF.GenderCode, StudentDF.InternationalStudentFlag, StudentDF.ProgramCode, 
                    StudentDF.ProgramDescs, StudentDF.StudentAddressCityCode, StudentDF.StudentAddressPostcode, StudentDF.StudentAddressStateCode, 
                    StudentDF.StudentCountryDesc, StudentDF.StudentID, StudentDF.TermCodes, DateDF.DateKey.alias('BirthDateSK'), 
                    DateDF.FullDateAlternateKey.alias('BirthDate'))

#One Student state for each term
StudentDF = StudentDF.select('AcademicLoadDesc', 'GenderCode', 'InternationalStudentFlag', 'ProgramCode', 'ProgramDescs', 
                            'StudentAddressCityCode', 'StudentAddressPostcode', 'StudentAddressStateCode', 'StudentCountryDesc',
                            'StudentID', explode(col('TermCodes')).alias('TermCode'), 'BirthDateSK')
StudentDF = StudentDF.withColumn('StudentAddressPostcode', when(isnull(StudentDF.StudentAddressPostcode), 
                                                                        StudentDF.StudentAddressCityCode.cast(IntegerType()))
                                                            .otherwise(StudentDF.StudentAddressPostcode))
StudentDF = StudentDF.withColumn('ProgramDesc', StudentDF.ProgramDescs.__getitem__(0).cast(StringType()))
StudentDF = StudentDF.withColumn('AddressDesc', concat_ws(',', StudentDF.StudentAddressCityCode, 
                                                            StudentDF.StudentAddressStateCode, StudentDF.StudentAddressPostcode))

StudentDF = StudentDF.groupBy(['StudentID', 'TermCode']).\
                        agg(collect_set(StudentDF.AcademicLoadDesc).alias('AcademicLoadDesc'), 
                        collect_set(StudentDF.GenderCode).__getitem__(0).cast(StringType()).alias('GenderCode'),
                        collect_set(StudentDF.InternationalStudentFlag).__getitem__(0).cast(StringType()).alias('InternationalStudentFlag'), 
                        collect_set(StudentDF.ProgramCode).alias('ProgramCode'),
                        collect_set(StudentDF.ProgramDesc).alias('ProgramDesc'), 
                        collect_set(StudentDF.StudentAddressCityCode).alias('StudentAddressCityCode'),
                        collect_set(StudentDF.StudentAddressPostcode).alias('StudentAddressPostcode'),
                        collect_set(StudentDF.StudentAddressStateCode).alias('StudentAddressStateCode'), 
                        collect_set(StudentDF.AddressDesc).alias('AddressDesc'),
                        collect_set(StudentDF.StudentCountryDesc).alias('StudentCountryDesc'),
                        collect_set(StudentDF.BirthDateSK).__getitem__(0).cast(IntegerType()).alias('BirthDateSK'))

StudentDF = StudentDF.withColumn('AcademicLoadDesc', when(size(StudentDF.AcademicLoadDesc) == 0, lit(None))
                                                    .otherwise(StudentDF.AcademicLoadDesc))
StudentDF = StudentDF.withColumn('ProgramCode', when(size(StudentDF.ProgramCode) == 0, lit(None))
                                                    .otherwise(StudentDF.ProgramCode))
StudentDF = StudentDF.withColumn('StudentAddressStateCode', when(size(StudentDF.StudentAddressStateCode) == 0, lit(None))
                                                    .otherwise(StudentDF.StudentAddressStateCode))
StudentDF = StudentDF.withColumn('StudentCountryDesc', when(size(StudentDF.StudentCountryDesc) == 0, lit(None))
                                                    .otherwise(StudentDF.StudentCountryDesc))

#Broadcast df header to workers
columnNames = spark.sparkContext.broadcast(StudentDF.columns)

#Self defined mapper
def mergeState(states):
    stateDF = pd.DataFrame(data=states, columns=columnNames.value)

    stateDF = stateDF.sort_values(by='TermCode').fillna(method='ffill').fillna(method='bfill').reset_index(drop=True)
    stateDF = stateDF.rename(columns={'TermCode': 'EffectiveTerm'})
    stateDF['ExpiryTerm'] = 0
    stateDF['keep'] = True

    index = 0
    sindex = 1
    columns = ['AcademicLoadDesc', 'GenderCode', 'InternationalStudentFlag', 'ProgramCode', 'StudentAddressCityCode',
                'StudentAddressStateCode', 'StudentCountryDesc', 'BirthDateSK']
    
    while sindex < stateDF.shape[0]:
        if stateDF.loc[index, columns].equals(stateDF.loc[sindex, columns]):
            stateDF.loc[sindex, 'keep'] = False
        else:
            stateDF.loc[index, 'ExpiryTerm'] = stateDF.loc[sindex, 'EffectiveTerm']
            index = sindex
            
        sindex = sindex + 1
    
    stateDF.loc[stateDF[stateDF['keep']].index[-1], 'ExpiryTerm'] = 9999
    return stateDF[stateDF['keep']].to_dict(orient='record')

#Outpout
newSchema = StructType([StructField('StudentID', StringType(), True),
                        StructField('EffectiveTerm', IntegerType(), True),
                        StructField('AcademicLoadDesc', ArrayType(StringType(), True), True),
                        StructField('GenderCode', StringType(), True),
                        StructField('InternationalStudentFlag', StringType(), True),
                        StructField('ProgramCode', ArrayType(StringType(), True), True),
                        StructField('ProgramDesc', ArrayType(StringType(), True), True),
                        StructField('StudentAddressCityCode', ArrayType(StringType(), True), True),
                        StructField('StudentAddressPostcode', ArrayType(IntegerType(), True), True),
                        StructField('StudentAddressStateCode', ArrayType(StringType(), True), True),
                        StructField('AddressDesc', ArrayType(StringType(), True), True),
                        StructField('StudentCountryDesc', ArrayType(StringType(), True), True),
                        StructField('BirthDateSK', IntegerType(), True),
                        StructField('ExpiryTerm', IntegerType(), True),
                        StructField('keep', BooleanType(), True)])

StudentDF = spark.createDataFrame(StudentDF.rdd.map(lambda r: (r['StudentID'], r))\
                .groupByKey().mapValues(mergeState)\
                .flatMap(lambda x: x[1]), schema=newSchema)

#Unpack the arrays
StudentDF = StudentDF.withColumn('MajorProgramCode', StudentDF.ProgramCode.__getitem__(0))
StudentDF = StudentDF.withColumn('SecondaryProgramCode', when(size(StudentDF.ProgramCode)>1, StudentDF.ProgramCode.__getitem__(1))
                                                        .otherwise(lit(None)))

StudentDF = StudentDF.withColumn('City1', StudentDF.StudentAddressCityCode.__getitem__(0))
StudentDF = StudentDF.withColumn('City2', when(size(StudentDF.StudentAddressCityCode)>1, StudentDF.StudentAddressCityCode.__getitem__(1))
                                        .otherwise(lit(None)))

StudentDF = StudentDF.withColumn('Postcode1', StudentDF.StudentAddressPostcode.__getitem__(0))
StudentDF = StudentDF.withColumn('Postcode2', when(size(StudentDF.StudentAddressPostcode)>1, StudentDF.StudentAddressPostcode.__getitem__(1))
                                        .otherwise(lit(None)))

StudentDF = StudentDF.withColumn('StateProvince1', StudentDF.StudentAddressStateCode.__getitem__(0))
StudentDF = StudentDF.withColumn('StateProvince2', when(size(StudentDF.StudentAddressStateCode)>1, StudentDF.StudentAddressStateCode.__getitem__(1))
                                        .otherwise(lit(None)))

StudentDF = StudentDF.withColumn('Country1', StudentDF.StudentCountryDesc.__getitem__(0))
StudentDF = StudentDF.withColumn('Country2', when(size(StudentDF.StudentCountryDesc)>1, StudentDF.StudentCountryDesc.__getitem__(1))
                                        .otherwise(lit(None)))

StudentDF = StudentDF.withColumn('AcademicLoadDesc', when(size(StudentDF.AcademicLoadDesc)==1, StudentDF.AcademicLoadDesc.__getitem__(0))
                                                    .otherwise(lit('Unknown')))

StudentDF = StudentDF.drop('ProgramCode', 'StudentAddressCityCode', 'StudentAddressPostcode', 
                            'StudentAddressStateCode', 'StudentCountryDesc', 'ProgramDesc', 'AddressDesc')

StudentDF = StudentDF.withColumn('StudentSK', row_number().over(w)).drop(col('keep'))
StudentDF.write.mode("overwrite").saveAsTable("unisadw.DimStudent")