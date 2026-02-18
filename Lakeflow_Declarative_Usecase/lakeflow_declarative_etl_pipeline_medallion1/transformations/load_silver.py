from pyspark import pipelines as dp
from pyspark.sql.functions import *

#read staff records from bronze and do standardization with sql select
@dp.table(name="silver_staff1",
          comment="standardized staff records",
          table_properties={"quality":"silver"})
@dp.expect_all_or_drop({
    "valid_shipment_id":"shipment_id is not null",
    #"valid_name":"full_name is not null" -- leads to data loss in case of only last_name is present in source
    })
def transform_to_silver_staff():
    return (
        spark.readStream.table("bronze_staff1")
        .dropDuplicates(["shipment_id"])
        .select(
            col("shipment_id").cast("bigint").alias("Shipment_ID"),
            concat_ws(" ",col("first_name"),col("last_name")).alias("Full_Name"),
            col("age").cast("int").alias("Age"),
            lower(col("role")).alias("Role"),
            initcap(col("hub_location")).alias("Hub_Location"),
            current_timestamp().alias("Load_Dt")
        )
    )

#read geo records from bronze and do standardization with sql select
@dp.table(name="silver_geotag1",
          comment="standardized geo records",
          table_properties={"quality":"silver"}
          )
@dp.expect_all_or_drop({
    "Valid_City":"city_name is not null",
    "Valid_lat":"latitude >= -90 and latitude <= 90",
    "Valid_long":"longitude >= -180 and longitude <= 180"
})
def transform_to_silver_geotag():
    return(
        spark.readStream.table("bronze_geotag1")
        .dropDuplicates(["city_name"])
        .select(
            initcap(col("city_name")).alias("City_Name"),
            initcap(col("country")).alias("Country"),
            col("latitude").alias("Latitude"),
            col("longitude").alias("Longitude")
        )
    )

#read shipment json and do transformation using withcolumns in silver load
@dp.table(
    name="silver_shipments1",
    comment="Enriched and split shipments data",
    table_properties={"quality": "silver"}
)
def silver_shipments_dlt2():
    ship_date_col = to_date(col("shipment_date"), "yy-MM-dd")    
    return (
        spark.readStream.table("bronze_shipment1")
        .withColumn("domain", lit("Logistics"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("is_expedited_flag_initial", lit(False).cast("boolean"))
        .withColumn("shipment_date_clean", ship_date_col)
        .withColumn("shipment_cost_clean", round(col("shipment_cost"), 2))
        .withColumn("shipment_weight_clean", col("shipment_weight_kg").cast("double"))
        .withColumn("route_segment", concat_ws("-", col("source_city"), col("destination_city")))
        .withColumn("vehicle_identifier", concat_ws("_", col("vehicle_type"), col("shipment_id")))
        .withColumn("shipment_year", year(ship_date_col))
        .withColumn("shipment_month", month(ship_date_col))
        .withColumn("is_weekend", 
            when(dayofweek(ship_date_col).isin([1, 7]), True)
            .otherwise(False)
        )
        .withColumn("is_expedited", 
            when(col("shipment_status").isin(["IN_TRANSIT", "DELIVERED"]), True)
            .otherwise(False)
        )
        .withColumn("cost_per_kg", round(col("shipment_cost") / col("shipment_weight_kg"), 2))
        .withColumn("tax_amount", round(col("shipment_cost") * 0.18, 2))
        .withColumn("days_since_shipment", datediff(current_date(), ship_date_col))
        .withColumn("is_high_value", 
            when(col("shipment_cost") > 50000, True)
            .otherwise(False))
        .withColumn("order_prefix", substring(col("order_id"), 1, 3))
        .withColumn("order_sequence", substring(col("order_id"), 4, 10))
        .withColumn("ship_day", dayofmonth(ship_date_col))
        .withColumn("route_lane", concat_ws("->", col("source_city"), col("destination_city"))))
