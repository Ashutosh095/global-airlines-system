# Databricks notebook source
df_airlines=spark.read.parquet('/mnt/containerprojectglobalairlines/bronze_zone/airlines')
df_airlines.printSchema()
display(df_airlines)

# COMMAND ----------

df_airplanes=spark.read.parquet('/mnt/containerprojectglobalairlines/bronze_zone/airplanes')
df_airplanes.printSchema()
display(df_airplanes)

# COMMAND ----------

df_routes=spark.read.parquet('/mnt/containerprojectglobalairlines/bronze_zone/routes')
df_routes.printSchema()
display(df_routes)

# COMMAND ----------

df_airports=spark.read.parquet('/mnt/containerprojectglobalairlines/bronze_zone/airports')
df_airports.printSchema()
display(df_airports)

# COMMAND ----------

df_na_airlines = df_airlines.na.fill('NA')
df_na_airlines.show()

# COMMAND ----------

df_na_airplanes = df_airplanes.na.fill('NA')
df_na_airplanes.show()

# COMMAND ----------

df_na_routes = df_routes.na.fill('NA')
df_na_routes.show()

# COMMAND ----------

df_na_airports = df_airports.na.fill('NA')
df_na_airports.show()

# COMMAND ----------

df_distinct_airlines=df_na_airlines.distinct()
df_distinct_airplanes=df_na_airplanes.distinct()
df_distinct_routes=df_na_routes.distinct()
df_distinct_airports=df_na_airports.distinct()

# COMMAND ----------

print('airlines\ntotal no. of rows:',df_na_airlines.count(),'\tno. of distinct rows:',df_distinct_airlines.count())
print('airplanes\ntotal no. of rows:',df_na_airplanes.count(),' \tno. of distinct rows:',df_distinct_airplanes.count())
print('routes\ntotal no. of rows:',df_na_routes.count(),'\tno. of distinct rows:',df_distinct_routes.count())
print('airports\ntotal no. of rows:',df_na_airports.count(),'\tno. of distinct rows:',df_distinct_airports.count())

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col,lit

def sanitize_column_names(df):
    characters_to_replace = r"[\(\)\{\},;\n\t\=\s]"
    df_schema = df.schema
    for column in df.columns:
        if df_schema[column].dataType == 'string': 
            df = df.withColumn(column, regexp_replace(col(column), characters_to_replace, "_"))
            df = df.withColumn(column, regexp_replace(col(column), r'\\N','NA'))
        new_col = regexp_replace(lit(column), characters_to_replace, "_").alias("new_col").cast("String")
        new_col_name = df.select(new_col).first()["new_col"]
        df = df.withColumnRenamed(column, new_col_name)              
    return df

# COMMAND ----------

df_distinct_airlines.count()

# COMMAND ----------

from pyspark.sql.functions import col,trim;

df_cleansed_airlines = df_distinct_airlines.filter(
    (col('Name')!='NA')&
    (col('Airline ID')!= -1) & 
    ((((trim(col('IATA'))!='NA'))&(trim(col('IATA'))!='-'))|
    (((trim(col('ICAO'))!='NA'))&(trim(col('ICAO'))!='-')))
)    
df_cleansed_airlines.count()

# COMMAND ----------

df_cleansed_airlines = sanitize_column_names(df_cleansed_airlines)

# COMMAND ----------

df_cleansed_airlines.printSchema()
display(df_cleansed_airlines.orderBy('Airline_ID'))

# COMMAND ----------

df_distinct_airplanes.count()

# COMMAND ----------

df_cleansed_airplanes=df_distinct_airplanes.filter(
    (col('Name')!='NA')&(
    ((trim(col('IATA code'))!='NA')& (trim(col('IATA code'))!='-'))| 
    (((trim(col('ICAO code'))!='NA'))& (trim(col('ICAO code'))!='-')))
)
df_cleansed_airplanes.count()

# COMMAND ----------

df_cleansed_airplanes = sanitize_column_names(df_cleansed_airplanes)

# COMMAND ----------

display(df_cleansed_airplanes.orderBy('Name'))

# COMMAND ----------

df_distinct_airports.count()

# COMMAND ----------

df_cleansed_airports = df_distinct_airports.filter(
    (col('Name')!='NA')&(
    ((col('latitude') >= -90) & (col('latitude') <= 90)) &
    ((col('longitude') >= -180) & (col('longitude') <= 180)) &
    ((trim(col('IATA')) != 'NA') & (trim(col('IATA')) != '-')) |
    ((trim(col('ICAO')) != 'NA') & (trim(col('ICAO')) != '-')))
)
df_cleansed_airports.count()

# COMMAND ----------

df_cleansed_airports = sanitize_column_names(df_cleansed_airports)

# COMMAND ----------

display(df_cleansed_airports.orderBy('Airport_ID'))

# COMMAND ----------

df_distinct_routes.count()

# COMMAND ----------

df_cleansed_routes = df_distinct_routes.filter(
    ((trim(col('Source airport')) != 'NA') & (trim(col('Source airport')) != '-')) |
    ((trim(col('Destination airport')) != 'NA') & (trim(col('Destination airport')) != '-'))
)
df_cleansed_routes.count()

# COMMAND ----------

df_cleansed_routes = sanitize_column_names(df_cleansed_routes)

# COMMAND ----------

display(df_cleansed_routes.orderBy('Airline'))

# COMMAND ----------

#Saving as delta format files
df_cleansed_airlines.write.mode('append').format('delta').save('/mnt/containerprojectglobalairlines/silver_zone/airlines')
df_cleansed_airplanes.write.mode('append').format('delta').save('/mnt/containerprojectglobalairlines/silver_zone/airplanes')
df_cleansed_airports.write.mode('append').format('delta').save('/mnt/containerprojectglobalairlines/silver_zone/airports')
df_cleansed_routes.write.mode('append').format('delta').save('/mnt/containerprojectglobalairlines/silver_zone/routes')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists db_global_airlines_system;

# COMMAND ----------

# MAGIC %sql
# MAGIC use db_global_airlines_system;

# COMMAND ----------

#Saving as deltatables
df_cleansed_airlines.write.mode('append').format('delta').option('path','/mnt/containerprojectglobalairlines/silver_zone/deltatbl_airlines/')\
    .saveAsTable('airlines')
df_cleansed_airplanes.write.mode('append').format('delta').option('path','/mnt/containerprojectglobalairlines/silver_zone/deltatbl_airplanes')\
    .saveAsTable('airplanes')
df_cleansed_airports.write.mode('append').format('delta').option('path','/mnt/containerprojectglobalairlines/silver_zone/deltatbl_airports')\
    .saveAsTable('airports')
df_cleansed_routes.write.mode('append').format('delta').option('path','/mnt/containerprojectglobalairlines/silver_zone/deltatbl_routes')\
    .saveAsTable('routes')

# COMMAND ----------

#%sql
#drop table db_global_airlines_system.airlines;
#drop table db_global_airlines_system.airplanes;
#drop table db_global_airlines_system.routes;
#drop table db_global_airlines_system.airports;
