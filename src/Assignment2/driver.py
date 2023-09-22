import pyspark
from util import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=create_SparkSession()
# 10.Create a non-nested dataframe with product, amount and country fields.
schema2= StructType([
    StructField('Product',StringType(),True),
    StructField('Amount',IntegerType(),True),
    StructField('Country',StringType(),True)
])

data2 =[
    ('Banana',1000,'USA'),
    ('Carrots',1500,'INDIA'),
    ('Beans',1600,'Sweden'),
    ('Orange',2000,'UK'),
    ('Orange',2000,'UAE'),
    ('Banana',400,'China'),
    ('Carrots',1200,'China')
]
df=create_dataframe(spark,data2,schema2)

#11.Find total amount exported to each country of each product.
df_pivot=df.groupBy('Country').pivot('Product').agg(sum('Amount'))


# 12.Perform unpivot function on output of question 2.
df_unpivot = unpivot_fun(df_pivot)
