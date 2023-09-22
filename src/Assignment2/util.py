import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_SparkSession():
    spark=SparkSession.builder.appName('Assignment2').getOrCreate()
    return spark

def create_dataframe(spark,data2,schema2):
    df=spark.createDataFrame(data=data2,schema=schema2)
    return df

def pivot_fun(df,Amount,Country,Product):
    df_pivot=df.groupBy(Product).pivot(Country).agg(sum(Amount))
    return df_pivot
def unpivot_fun(pivot_fun):
    df_unpivot=pivot_fun.select('Country', expr("stack(4,'Banana',Banana,'Beans',Beans,'Carrots',Carrots,'Orange',Orange) as (Product,Amount)")).where("Amount is not null")
    return df_unpivot