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
    df_select=df.select(firstname,lastname,salary)
    return df_select

def add_col(df,col_name,col_value):
    df_add=df.withColumn(col_name,lit(col_value))
    return df_add

def change_value(df,salary,value):
    df_change=df.withColumn("salary",col("salary")+value)
    return df_change

def change_datatype(df,dob,salary):
    df_change_datatype=df.withColumn("dob", col("dob").cast("String")).withColumn("salary", col("salary").cast("String"))
    return df_change_datatype

def add_column(df,bonus,salary,value):
    df_newcol=df.withColumn("bonus",col(salary)+value)
    return df_newcol

def nested_column(df):
    df_rename=df.withColumn('name', expr("map('firstposition', name['firstname'], 'middleposition', name['middlename'], 'lastposition', name['lastname'])"))
    return df_rename

def max_sal(df,name,salary):
    # df7=df.select("name").filter(col(column) == max(col(column)))
    df_filter_maxsal=df.filter( (col(name) == max(salary)) )
    return df_filter_maxsal

def drop_column(df,column):
    df_drop=df.drop(column)
    return df_drop

def distinct_value(df,column):
    df_distinct=df.select(column).distinct()
    return df_distinct