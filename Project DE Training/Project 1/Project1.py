# Databricks notebook source
storage_account_name = "project1storage1074"
container_name = "source1074"
mount_point = "/mnt/project1-1074"
# Use the access key obtained in Step 1
storage_account_key= "YCbuS+VxTit8iBOytYEgemrm46b1qQV9qsRiunE6rKpqg0e3+Fku7UO3ukxh9cuknVJJgIfIEJVQ+AStG94B2w=="
# Create the mount point
dbutils.fs.mkdirs(mount_point)
# Mount Azure Storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = mount_point,
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)


# COMMAND ----------

dbutils.fs.ls("/mnt/project1-1074")

# COMMAND ----------

df=spark.read.load("dbfs:/mnt/project1-1074/cars.csv",format="csv",header="True",inferschema="True")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("csv").option("header", True).load("dbfs:/mnt/project1-1074/cars.csv")

#counting rows
rowcount=df.count()
print({rowcount})

# COMMAND ----------

storage_account_name = "destination1project"
container_name = "destination1"
mount_point = "/mnt/desti-1074"
# Use the access key obtained in Step 1
storage_account_key= "jeEe0KyIWKeQ/2Usq1Bgcsnf6BzfOdlz8nt4wzNH5nyVf9mAhVnlu6W5iA8cK8E7vP7hD3LOtMQ++AStCXAHKA=="
# Create the mount point
dbutils.fs.mkdirs(mount_point)
# Mount Azure Storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = mount_point,
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

df.write.mode("overwrite").csv("/mnt/desti-1074/transformed_file.csv")

# COMMAND ----------

dbutils.fs.unmount("/mnt/project1-1074") 
