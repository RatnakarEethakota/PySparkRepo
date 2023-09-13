import pyspark
from util import *
from pyspark.sql import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField

spark=create_SparkSession()

schema= StructType([
    StructField("name",StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True)])),
    StructField("dob",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
])
data = [
    (('james','','Smith'),'03011998','M',3000),
    (('Micheal','Rose',''),'10111998','M',20000),
    (('Robert','','Williams'),'02012000','M',3000),
    (('Maria','Anne','Jones'),'03011998','F',11000),
    (('Jen','Mary','Brown'),'04101998','F',10000)
]
df=create_dataframe(spark,data,schema)
df.show()

#1.	Select firstname, lastname and salary from Dataframe.
df1=select(df,df.name.firstname,df.name.lastname,df.salary)


# 2. Add Country, department, and age column in the dataframe.
df2=add_col(df,col_name="Country",col_value="INDIA")
df2=add_col(df,col_name="department",col_value="IT Department")
df2=add_col(df,col_name="age",col_value="24")

# 3. Change the value of salary column
df3=change_value(df,salary='salary',value=1000)


#4.	Change the data types of DOB and salary to String
df4=change_datatype(df,dob='String',salary='String')

# 5.Derive new column from salary column.
df5=add_column(df,bonus='bonus',salary='salary',value=500)

# 6.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
df6=nested_column(df,firstname='firstposition',middlename='secondposition',lastname='lastposition')

# 7.Filter the name column whose salary in maximum.
df7=max_sal(df,column='name')


# 8.Drop the department and age column.
df8=drop_column(df,column=('department'))
df8=drop_column(df,column=('age'))

# 9.List out distinct value of dob and salary
df9=distinct_value(df,column='dob')
df9=distinct_value(df,column='salary')