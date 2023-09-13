import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_SparkSession():
    spark = SparkSession.builder.appName('Assignment1').getOrCreate()
    return spark
def create_dataframe(spark,data,schema):
    df=spark.createDataFrame(data=data, schema=schema)
    return df
def select(df,firstname,lastname,salary):
    df1=df.select(firstname,lastname,salary)
    return df1
def add_col(df,col_name,col_value):
    df2=df.withColumn(col_name,lit(col_value))
    return df2
def change_value(df,salary,value):
    df3=df.withColumn("salary",col("salary")+value)
    return df3
def change_datatype(df,dob,salary):
    df4=df.withColumn("dob", col("dob").cast("String"))
    df4=df.withColumn("salary", col("salary").cast("String"))
    return df4
def add_column(df,bonus,salary,value):
    df5=df.withColumn("bonus",col(salary)+value)
    return df5
def nested_column(df,firstname,middlename,lastname):
    df6=df.withColumnRenamed(firstname, 'firstposition'),(middlename, 'secondposition'),(lastname, 'lastposition')
    return df6
def max_sal(df,column):
    df7=df.select(max(column))
    return df7
def drop_column(df,column):
    df8=df.drop(column)
    return df8
def distinct_value(df,column):
    df9=df.select(column).distinct()
    return df9