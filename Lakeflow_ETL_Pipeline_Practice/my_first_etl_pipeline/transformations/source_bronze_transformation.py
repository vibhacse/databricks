from pyspark import pipelines as dp

@dp.table()
def import_src_to_bronze():
    df1=spark.read.table("mysql_foreign_cat.logistics.shipments1")
    df2=df1.filter(df1.city.isNotNull())
    return df2