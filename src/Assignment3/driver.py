import pyspark
from util import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=create_SparkSession()

schema1 = StructType([
    StructField('employee_name',StringType(),True),
    StructField('department',StringType(),True),
    StructField('salary',IntegerType(),True)
])

data1 = [
    ('James','Sales',3000),
    ('Michael','Sales',4600),
    ('Robert','Sales',4100),
    ('Maria','Finance',3000),
    ('Raman','Finance',3000),
    ('Scott','Finance',3300),
    ('Jen','Finance',3900),
    ('Jeff','Marketing',3000),
    ('Kumar','Marketing',2000)
]
df=create_dataframe(spark,data1,schema1)

# Select first row from each department group.
df1=first_row(df).show()

# Retrieve Employees who earns the highest salary.
df2=highest_salary(df)
df2.show()

# Select the highest, lowest, average, and total salary for each department group.
df3=totalsal_avg_high_low(df)
df3.show()

