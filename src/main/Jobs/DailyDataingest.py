from datetime import datetime, date, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import configparser
from src.main.python.gkfunctions import read_schema
import pyspark.sql.functions as fun

config = configparser.ConfigParser()

config.read(r"../projectconfigs/config.ini")

inputlocation = config.get('paths', 'inputlocation')
outputlocation = config.get('paths', 'outputlocation')

# initiating spark Session

spark = SparkSession.builder.appName("DataInjectAnd Refine").master("local").getOrCreate()

#  creating schema for csv file

landingFileSchemaFromConf = config.get("schema", "landingFileSchema")
holdFileSchemaFromConf = config.get("schema", "HoldFileSchema")

landingFileSchema = read_schema(landingFileSchemaFromConf)

holdFileSchema = read_schema(holdFileSchemaFromConf)

# Creating new Data frame for landing file data

# newDF = spark.read.csv("G:\ganesh\GK codelabs data\Batch-processing project\GKCodelabs-BigData-Batch-Processing-Course\Data\Inputs\Sales_Landing\SalesDump_04062020")

today = datetime.now()

yesterday = today - timedelta(1)

print(today)
print(yesterday)

# currentdate = today.strftime("_%d%m%Y")
# yesterdaydate =yesterday.strftime("_%d%m%Y")

currentdate = "_04062020"
yesterdaydate = "_03062020"

print(currentdate)
print(yesterdaydate)

landingFileDF = spark \
    .read \
    .schema(landingFileSchema) \
    .option("delimiter", "|").csv(inputlocation + "Sales_Landing\SalesDump" + currentdate)

landingFileDF.createOrReplaceTempView("landingFileDF")
# reading previous hold data

previousHoldDF = spark.read \
    .schema(holdFileSchema) \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(outputlocation + "Hold/invalidData" + yesterdaydate)

previousHoldDF.createOrReplaceTempView("previousHoldDF")

refreshedLandingData = spark.sql("select a.Sale_ID, a.Product_ID, "
                                 "CASE "
                                 "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
                                 "ELSE a.Quantity_Sold "
                                 "END AS Quantity_Sold, "
                                 "CASE "
                                 "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID "
                                 "ELSE a.Vendor_ID "
                                 "END AS Vendor_ID, "
                                 "a.Sale_Date, a.Sale_Amount, a.Sale_Currency "
                                 "from landingFileDF a left outer join previousHoldDF b "
                                 "on a.Sale_ID = b.Sale_ID" )

# refreshedLandingData.show()


validlandingfileDF = refreshedLandingData.filter(fun.col("Quantity_Sold").isNotNull() & (fun.col("Vendor_ID").isNotNull()))

validlandingfileDF.createOrReplaceTempView("validlandingfileDF")

releasedFromHold = spark.sql("select a.Sale_ID "
                             "from validlandingfileDF a INNER JOIN previousHoldDF b "
                             "on a.Sale_ID = b.Sale_ID ")

releasedFromHold.createOrReplaceTempView("releasedFromHold")

notReleasedFromHold = spark.sql("select * from "
                                "previousHoldDF where Sale_ID not in( select Sale_ID from releasedFromHold)")

# releasedFromHold.show()
# notReleasedFromHold.show()

invalidlandingfileDF = refreshedLandingData.filter(fun.col("Quantity_Sold").isNull() | fun.col("Vendor_ID").isNull() |
                                                 fun.col("Sale_Currency").isNull())\
    .withColumn("Hold_Reason", fun
                .when(fun.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(fun.when(fun.col("Vendor_ID").isNull(), "Vendor ID Missing")))\
    .union(notReleasedFromHold)

# invalidlandingfileDF.show()
invalidlandingfileDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputlocation+"Hold/invalidData"+currentdate)

validlandingfileDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputlocation+"Valid/validData"+currentdate)

