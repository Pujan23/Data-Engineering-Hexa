# Databricks notebook source
storage_account_name = "project2storage1074"
container_name = "sourcecontainer1074"
mount_point = "/mnt/project2-1074"

# Use the access key obtained in Step 1
storage_account_key = "Xhyio96Y94VNWdB35J3Hce/GD4f3MuI0z4FdI5AyWNV9i5n8ZqCryboJHS/Q4oztVe48oR9Eu2Be+AStUfzcSw=="

# Create the mount point
dbutils.fs.mkdirs(mount_point)

# Mount Azure Storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = mount_point,
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)


# COMMAND ----------

dbutils.fs.ls("/mnt/project2-1074")

# COMMAND ----------

df=spark.read.load("dbfs:/mnt/project2-1074/cars.csv",format="csv",header="True",inferschema="True")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("csv").option("header", True).load("dbfs:/mnt/project2-1074/cars.csv")

#counting rows
rowcount=df.count()
print({rowcount})
