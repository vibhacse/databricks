from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@dp.table()
def load_bronze():

    schema1=StructType([
        StructField("id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    data = [
        (1,'vivek','bharathi',35),
        (2,'john','doe',40),
        (3,'mary','jane',45),
        (4,'jane','doe',50)
    ]
    
    return spark.createDataFrame(data, schema1)