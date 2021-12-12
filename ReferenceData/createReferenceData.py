import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date
import sys

# initializing spark session
spark = SparkSession \
    .builder \
    .appName("ReferenceDataCreation") \
    .getOrCreate()

# defining PySpark UDF to clean leading and trailing special characters from strings
def removeSpecialChar(x):
    x = x.strip('\"\n%#?*- ')
    x = x.replace('\"', '')
    if x.endswith('('):
        x = x[:-1]
    return x

removeSpecialChar_udf = udf(removeSpecialChar, StringType())

def cleanReferenceDataColumn(column_name, input_data):
    # make all vaues uppercase to remove duplicates by case 
    input_data = input_data.withColumn(column_name,F.upper(F.col(column_name)))
    # remove any trailing or leading special characters or whitespaces
    input_data = input_data.withColumn(column_name, removeSpecialChar_udf(input_data[column_name]))
    # there are entries with sentences instead of name, filter them out
    input_data = input_data.filter(length(col(column_name)) < 25)
    return input_data


if __name__ == "__main__":
    path = '/user/CS-GY-6513/project_data/data-cityofnewyork-us.pqg4-dm6b.csv'
    input_data = spark.read.format('csv').options(header='true',inferschema='true').load(path)

    for column in input_data.columns:
        if column == "OrganizationName":
            input_data = cleanReferenceDataColumn(column, input_data)

    # select only the Organization name column to output
    input_data.select([col("OrganizationName").alias("Name")]).repartition(1).write.csv("NGONameReferenceDataset.csv",header=True)




