from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
from src.main.python.gkfunctions import read_schema
import configparser

spark = SparkSession.builder.appName("abc").getOrCreate()

config = configparser.ConfigParser()
config.read(r"../projectconfigs/config.ini")

inputlocation = config.get("paths", "inputlocation")
outputlocation = config.get("paths", "outputlocation")
schemafromconf = config.get("schema", "landingFileSchema")
referenceschemafromconf = config.get("schema", "productReferenceSchema")

currentdate = "_04062020"
yesterdaydate = "_03062020"

landingFileSchema = read_schema(schemafromconf)
productReferenceSchema = read_schema(referenceschemafromconf)

validData = spark.read \
    .option("delimiter", "|") \
    .option("header", True) \
    .schema(landingFileSchema) \
    .csv(outputlocation + "Valid/validData" + currentdate)

# validData.show()
validData.createOrReplaceTempView("validData")

productPriceReferenceDF = spark.read \
    .schema(productReferenceSchema) \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(inputlocation + "Products")

productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")
# productPriceReferenceDF.show()

enrichDataDF = spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, "
                              "a.Quantity_Sold, a.Vendor_ID, a.Sale_Date, "
                              "b.Product_Price * a.Quantity_Sold as Sale_Amount,"
                              "a.Sale_Currency "
                              "from validData a INNER JOIN productPriceReferenceDF b "
                              "ON a.Product_ID = b.Product_ID")

enrichDataDF.show()

enrichDataWithCurrencyExchangeDF = spark.sql("select a.Sale_ID, a.Product_ID, a.Quantity_Sold, "
                                             "b.Product_Price, "
                                             "case "
                                             "when (a.sale_Currency = 'USD') THEN (a.Quantity_Sold * b.Product_Price)*80 "
                                             "ELSE (a.Quantity_Sold * b.Product_Price) "
                                             "END as Sale_Amount, "
                                             " a.Sale_Currency, a.Sale_Date "
                                             "from validData a inner join productPriceReferenceDF b "
                                             "on a.Product_ID = b.Product_ID")

# enrichDataWithCurrencyExchangeDF = spark.sql("select a.Sale_ID, a.Product_ID, a.Quantity_Sold, a.Sale_Currency, "
#                                              "b.Product_Price, "
#                                              "case "
#                                              "when (a.sale_Currency = 'USD') THEN (a.Quantity_Sold * b.Product_Price)*80 "
#                                              "ELSE "
#                                              "case "
#                                              "When (a.sale_Currency = 'INR') THEN (a.Quantity_Sold * b.Product_Price) "
#                                              "END as Sale_Amount, "
#                                              "a.Sale_Date "
#                                              "from validData a inner join productPriceReferenceDF b "
#                                              "on a.Product_ID = b.Product_ID")

enrichDataWithCurrencyExchangeDF.show()

enrichDataDF.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(outputlocation + "Enriched/SaleAmountEnrichment/SaleAmountEnrichment" + currentdate)
