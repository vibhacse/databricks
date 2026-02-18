from pyspark import pipelines as dp

@dp.table(name="bronze_staff1")
@dp.expect("Valid_Vechicle","vehicle_type is not null")
def load_staff():
    return (
        spark.readStream.format("cloudFiles") #format of the file
        .option("cloudFiles.format","csv") #read options
        .option("inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger",1)
        .load("/Volumes/prodcatalog/logistics/source/staff") #source path
    )

@dp.table(name="bronze_geotag1")
def load_geotag():
    return (
        spark.readStream.format("cloudFiles") #format of the file
        .option("cloudFiles.format","csv") #read options
        .option("inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")
        .option("cloudFIles.maxFilesPerTrigger",1)
        .load("/Volumes/prodcatalog/logistics/source/geo") #source path
    )

@dp.table(name="bronze_shipment1")
def load_shipment_json():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("inferColumnTypes","true")
        .option("multiLine","true")
        .option("cloudFiles.maxFilesPerTrigger",1)
        .load("/Volumes/prodcatalog/logistics/source/shipment")
        .select(
            "shipment_id",
                "order_id",
                "source_city",
                "destination_city",
                "shipment_status",
                "cargo_type",
                "vehicle_type",
                "payment_mode",
                "shipment_weight_kg",
                "shipment_cost",
                "shipment_date"
        )
    )