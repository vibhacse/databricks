from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(name="gold_staff_geo_enriched_dlt1",
                      comment="Staff enriched with Geo Location data",
                      table_properties={"quality":"gold"}
                      )
def silver_to_gold_staff_geo1():
    staffdf = spark.read.table("silver_staff1")
    geodf = spark.read.table("silver_geotag1")
    return (
        staffdf.join(geodf, 
                     staffdf.Hub_Location==geodf.City_Name, 
                     "inner")
        .select(
            staffdf["*"],
            geodf.Latitude.alias("Geo_Latitude"),
            geodf.Longitude.alias("Geo_Longitude")
        )
)
    
@dp.materialized_view(
    name="gold_shipment_stats1",
    comment="Aggregated Shipment statistics by Source City",
    table_properties={"quality": "gold"}
)
def silver_gold_shipment_stats():
    return (
        spark.read.table("silver_shipments1")
        .groupBy("source_city")
        .agg(
            sum("shipment_cost_clean").alias("total_cost"),
            count("shipment_id").alias("total_shipments"),
            avg("shipment_weight_clean").alias("avg_weight")
        )
    )