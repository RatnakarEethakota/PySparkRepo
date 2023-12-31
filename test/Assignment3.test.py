import unittest
from src.Assignment3.util import  col,row_number,avg,sum,min,max,\
    SparkSession,first_row,highest_salary,\
    totalsal_avg_high_low,StringType,IntegerType,StructType,StructField,FloatType

data=[("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)]
schema=StructType([StructField("EmpName",StringType(),True),
                   StructField("Department",StringType(),True),
                   StructField("Salary",IntegerType(),True)])
class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment 2").getOrCreate()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def test_first_row(self):
        df=self.spark.createDataFrame(data=data,schema=schema)
        actual_df=first_row(df)
        expected_schema = StructType([
            StructField("EmpName", StringType(), True),
            StructField("Department", StringType(), True),
            StructField("Salary", IntegerType(), True)
        ])
        expected_data = [
            ("Maria", "Finance", 3000),
            ("Kumar", "Marketing", 2000),
            ("James", "Sales", 3000)
        ]
        df2=self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        self.assertEqual(actual_df.collect(),df2.collect())

    def test_highest_salary(self):
        df=self.spark.createDataFrame(data=data,schema=schema)
        actual_df=highest_salary(df)
        expected_data=[("Jen","Finance",3900),
                       ("Jeff","Marketing",3000),
                       ("Michael","Sales",4600)]
        expected_schema= StructType([StructField("EmpName",StringType(),True),
                                     StructField("Department",StringType(),True),
                                     StructField("Salary",IntegerType(),True)])

        expected_df= self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        self.assertEqual(actual_df.collect(),expected_df.collect())

    def test_totalsal_avg_high_low(self):
        df = self.spark.createDataFrame(data=data,schema=schema)
        actual_df = totalsal_avg_high_low(df)
        expected_data = [
            ("Finance", 3300.0, 3900, 3000, 13200),
            ("Marketing", 2500.0, 3000, 2000, 5000),
            ("Sales", 3900.0, 4600, 3000, 11700)
        ]
        expected_schema = StructType([
                            StructField("Department", StringType(), True),
                            StructField("Average", FloatType(), True),
                            StructField("highest_salary", IntegerType(), True),
                            StructField("lowest_salary", IntegerType(), True),
                            StructField("total_salary", IntegerType(), True)])
        expected_df=self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        self.assertEqual(actual_df.collect(),expected_df)

if __name__ == '__main__':
    unittest.main()