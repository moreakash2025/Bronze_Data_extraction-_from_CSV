# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize SparkSession with the necessary option to handle S3
spark = SparkSession.builder \
    .appName("AWS S3 Integration and Data Processing") \
    .getOrCreate()

# Set AWS S3 access keys securely
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZG4APFAQRRDBUO65")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# Define S3 bucket and file paths
bucket_name = "iiht-ntt-24"
customer_file_path = f"s3a://{bucket_name}/FileStore/tables/Customer.csv"

# Define the schema for the Customer CSV based on the initial definition provided
customer_schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("BillToCustomerID", StringType(), True),
    StructField("CustomerCategoryID", StringType(), True),
    StructField("BuyingGroupID", StringType(), True),
    StructField("PrimaryContactPersonID", StringType(), True),
    StructField("PostalCityID", StringType(), True),
    StructField("ValidFrom", DateType(), True),
    StructField("ValidTo", DateType(), True),
    StructField("LineageKey", IntegerType(), True)
])

# Read the Customer CSV file into a DataFrame using the defined schema
customer_df = spark.read.format("csv").schema(customer_schema).load("/FileStore/tables/Customer.csv")

# Transform the DataFrame as required
customer_df_transformed = customer_df.selectExpr(
    "CustomerID as CustomerKey",
    "CustomerID as WWICustomerID",
    "CustomerName as Customer",
    "BillToCustomerID as BillToCustomer",
    "CustomerCategoryID as Category",
    "BuyingGroupID as BuyingGroup",
    "PrimaryContactPersonID as PrimaryContact",
    "PostalCityID as PostalCode",
    "ValidFrom as ValidFrom",
    "ValidTo as ValidTo",
    "LineageKey as LineageKey"
).withColumn("LineageKey", lit(9))  # Set the value of LineageKey to 9


# Handle the SilverCustomer table creation or replacement with error handling
table_name = "SilverCustomer"
try:
    if spark._jsparkSession.catalog().tableExists(table_name):
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    customer_df_transformed.write.format("parquet").mode("overwrite").saveAsTable(table_name)
    print(f"Table {table_name} created/overwritten successfully.")
except Exception as e:
    print(f"Failed to create or replace table {table_name} due to: {e}")





