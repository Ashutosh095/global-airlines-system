# Databricks notebook source
dbutils.fs.ls("/mnt/containerprojectglobalairlines/")

# COMMAND ----------

filelist=dbutils.fs.ls("/mnt/containerprojectglobalairlines/landing_zone")
df_fileNames = spark.createDataFrame(filelist).select("name")
df_fileNames.show()

# COMMAND ----------

from pyspark.sql.functions import current_date

base_path = "/mnt/containerprojectglobalairlines/landing_zone/"

for row in df_fileNames.collect():
    file_name = row["name"]  
    file_path = base_path + file_name
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print(file_name)
    display(df)
    df=df.withColumn("processDate",current_date())
    df_name = file_name.split(".")[0]
    output_path = f"/mnt/containerprojectglobalairlines/bronze_zone/{df_name}"
    df.write.mode('append').partitionBy('processDate').parquet(output_path)
    dbutils.fs.rm(file_path)
