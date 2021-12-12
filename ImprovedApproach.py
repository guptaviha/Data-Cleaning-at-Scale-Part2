import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from dateutil import parser
from pyspark.sql.functions import to_date
import datetime
import sys

spark = SparkSession \
    .builder \
    .appName("ProjectImprovedApproach") \
    .getOrCreate()

# Since spark is not good with identifying date columns due to varying formats in different datasets, 
# we are using the below dict to correctly categorize such columns in the datasets we clean
datecolumns_dict = {
    'kpav-sd4t': ['Posting Date','Post Until','Posting Updated','Process Date'],
    'bdjm-n7q4': ['ClosedDate', 'CancelDate', 'CreatedDate', 'UpdatedDate', 'SanitationAssignedDate', 'SanitationRemovalDate', 'SanitationUpdatedDate', 'PROJSTARTDATE'],
    'p937-wjvj': ['INSPECTION_DATE', 'APPROVED_DATE'],
    'pqg4-dm6b': [],
    'bquu-z2ht': ['created_date', 'last_modified_date', 'start_date_date', 'end_date_date'],
    'nzjr-3966': [],
    '4d7f-74pe': [],
    'c5up-ki6j': ['contract_end', 'contract_start'],
    'dsg6-ifza': ['Permit Expiration', 'Date Permitted', 'Inspection Date'],
    '6khm-nrue': ['Data os of Date'],
    'vz8c-29aj': [],
    'ay9k-vznm': [],
    'qdq3-9eqn': ['Day'],
    'fudw-fgrp': ['date']
} 


# Original count and sample size of each dataset using confidence level 95% and confidence interval 8
samplesize_dict = {
    'kpav-sd4t': [2915, 143],
    'bdjm-n7q4': [825691, 150],
    'p937-wjvj': [2018180, 150],
    'pqg4-dm6b': [1147, 133],
    'bquu-z2ht': [665, 123],
    'nzjr-3966': [3023, 143],
    '4d7f-74pe': [104316, 150],
    'c5up-ki6j': [3406, 144],
    'dsg6-ifza': [29953, 149],
    '6khm-nrue': [155, 76],
    'vz8c-29aj': [14, 13],
    'ay9k-vznm': [197 , 85],
    'qdq3-9eqn': [2097151, 150],
    'fudw-fgrp': [74881, 150]
} 

def findColumnCategory(datasetID,column_name, input_data):
    if column_name.lower() in [x.lower() for x in datecolumns_dict[datasetID]]:
      return 'DATE'
    elif 'borough' in column_name.lower():
      return 'BOROUGH'
    elif re.search("(salary)|(hours?)|(rate)", column_name.lower()) and column_name not in ['Salary Frequency','Hours/Shift']:
      return 'NUMERIC'
    elif 'title' in column_name.lower():
      return 'TITLE'

def castToDate(column_name, input_data):
    try:
        input_data = input_data.withColumn(column_name, F.to_date(F.col(column_name)))
    except:
        pass
    return input_data

def standardizeDate(x):
    d = parser.parse(x)
    return d.strftime("%Y-%m-%d")

standardizeDate_udf = udf(standardizeDate, StringType())

def removeSpecialChar(x):
    x = x.strip("#?*- ")
    if x.endswith('('):
        x = x[:-1]
    return x

removeSpecialChar_udf = udf(removeSpecialChar, StringType())


def cleanAndProfileColumn(datasetID, column_name, input_data):
    type = findColumnCategory(datasetID, column_name, input_data)
    if type == 'DATE':
        standardized = False
        input_data_temp = input_data
        # standardize date format so that we can easilycast the column to Date datatype
        try:
            input_data_temp = input_data_temp.withColumn(column_name, F.when(input_data_temp[column_name].isNull(),'UNSPECIFIED').otherwise(standardizeDate_udf(F.col(column_name))) )
            input_data_temp.select(input_data_temp[column_name]).show()
            input_data = input_data_temp
            standardized = True
        except:            
            pass
        if standardized:
            # cast to date so that range queries can work
            input_data = castToDate(column_name, input_data)
            # filter out outliers
            input_data.createOrReplaceTempView('input_data_view')
            input_data = spark.sql("SELECT * FROM input_data_view a WHERE a.`" + column_name + "` >= '1965-01-01' and a.`" + column_name + "` <= '2040-01-01'")
    elif type == 'BOROUGH':
        input_data = input_data.withColumn(column_name,F.upper(F.col(column_name)))
        input_data = input_data.withColumn(column_name, F.when(input_data[column_name].isNull(),'UNSPECIFIED').otherwise(F.trim(input_data[column_name])))
        input_data = input_data.withColumn(column_name, F.when(input_data[column_name] == 'RICHMOND','STATEN ISLAND').otherwise(input_data[column_name]))
        input_data = input_data[(input_data[column_name] == 'BROOKLYN') | \
        (input_data[column_name] == 'MANHATTAN') | \
        (input_data[column_name] == 'BRONX') | \
        (input_data[column_name] == 'STATEN ISLAND') | \
        (input_data[column_name] == 'QUEENS') | \
        (input_data[column_name] == 'UNSPECIFIED')]
    elif type == "NUMERIC":
        input_data = input_data.withColumn(column_name, F.col(column_name).cast('float'))
        input_data = input_data[input_data[column_name] >= 0]
    elif type == "TITLE":
        input_data = input_data.withColumn(column_name,F.upper(F.col(column_name)))
        input_data = input_data.withColumn(column_name, removeSpecialChar_udf(input_data[column_name]))
    return input_data

def cleanAndProfileDataset(datasetID, testFlag):
    global spark
    path = '/user/CS-GY-6513/project_data/data-cityofnewyork-us.'+ datasetID + '.csv'
    input_data = spark.read.format('csv').options(header='true',inferschema='true').load(path)
    columns = input_data.columns

    # If testFlag is False, run the cleaning on entire dataset and store output in HDFS
    # Else do cleaning on the sample, and store the results in csv to calculate precision and recall
    if testFlag == "False":
        for column in columns:
            input_data = cleanAndProfileColumn(datasetID, column, input_data)

        # store output to HDFS
        input_data.write.save(datasetID + "Cleaned.out",format="csv",header=True)
    else:
        # Take subset of data to clean and find accuracy by means of precision and recall
        sampleSize = samplesize_dict[datasetID][1]
        count = samplesize_dict[datasetID][0]
        input_data_subset = input_data.sample(fraction=1.0*sampleSize/count, seed=1)

        # Store the original sample to HDFS so that it can be used to calculate precision and recall
        input_data_subset.write.save(datasetID + "Original",format="csv",header=True)

        for column in columns:
            input_data_subset = cleanAndProfileColumn(datasetID, column, input_data_subset)

        # store cleaned sample dataset to HDFS
        input_data_subset.write.save(datasetID + "Out",format="csv",header=True)


if __name__ == "__main__":
    cleanAndProfileDataset(sys.argv[1], sys.argv[2])
