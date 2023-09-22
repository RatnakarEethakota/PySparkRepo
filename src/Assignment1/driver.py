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
    StructField("dob",LongType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
])
data = [
    (('james','','Smith'),3011998,'M',3000),
    (('Micheal','Rose',''),10111998,'M',20000),
    (('Robert','','Williams'),2012000,'M',3000),
    (('Maria','Anne','Jones'),3011998,'F',11000),
    (('Jen','Mary','Brown'),4101998,'F',10000)
]
df=create_dataframe(spark,data,schema)
df.show()

#1.	Select firstname, lastname and salary from Dataframe.
df_select=select(df,df.name.firstname,df.name.lastname,df.salary)


# 2. Add Country, department, and age column in the dataframe.
df_add=add_col(df,col_name="Country",col_value="INDIA")
df_add=add_col(df,col_name="department",col_value="IT Department")
df_add=add_col(df,col_name="age",col_value="24")

# 3. Change the value of salary column
df_change=change_value(df,salary='salary',value=1000)


#4.	Change the data types of DOB and salary to String
df_change_datatype=change_datatype(df,dob='String',salary='String')


# 5.Derive new column from salary column.
df_newcol=add_column(df,bonus='bonus',salary='salary',value=500)

# 6.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
df_rename=nested_column(df).show()

# 7.Filter the name column whose salary in maximum.
df_filter_maxsal=max_sal(df,name='name',salary='salary')



# 8.Drop the department and age column.
df_drop=drop_column(df,column=('department'))
df_drop=drop_column(df,column=('age'))

# 9.List out distinct value of dob and salary
df_distinct=distinct_value(df,column='dob')
df_distinct=distinct_value(df,column='salary')