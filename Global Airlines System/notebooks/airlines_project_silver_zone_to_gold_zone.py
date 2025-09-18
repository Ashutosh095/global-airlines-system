# Databricks notebook source
dbutils.widgets.text("loadType", "")
loadType=dbutils.widgets.get("loadType")
print(loadType)

# COMMAND ----------

silver_zone_path='/mnt/containerproject/silver_zone/global_airlines_system/'
gold_zone_path='/mnt/containerproject/gold_zone/global_airlines_system/'
silverDB='db_global_airlines_system_silver'
goldDB='db_global_airlines_system_gold'

# COMMAND ----------

df_airlines=spark.read.format("delta").option('path',silver_zone_path+'deltaTbl/')\
    .table(silverDB+'.airlines').orderBy('Airline_ID')
df_airlines.printSchema()
display(df_airlines)

# COMMAND ----------

df_airplanes=spark.read.format("delta").option('path',silver_zone_path+'deltaTbl/')\
    .table(silverDB+'.airplanes').orderBy('Name')
df_airplanes.printSchema()
display(df_airplanes)

# COMMAND ----------

df_airports=spark.read.format("delta").option('path',silver_zone_path+'deltaTbl/')\
    .table(silverDB+'.airports').orderBy('Airport_ID')
df_airports.printSchema()
display(df_airports)

# COMMAND ----------

df_routes=spark.read.format("delta").option('path',silver_zone_path+'deltaTbl/')\
    .table(silverDB+'.routes').orderBy('Airline')
df_routes.printSchema()
display(df_routes)

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {goldDB}')
spark.sql(f'use {goldDB}')

# COMMAND ----------

from pyspark.sql.functions import *
def createDeltaFL(df,filename,foldername):
    df.write.format('delta').mode('overwrite').option('path',gold_zone_path+foldername+'/'+filename)\
        .saveAsTable(goldDB+'.'+filename)
    df.write.format('delta').mode('overwrite').save(gold_zone_path+filename)

def createDeltaIL(df,filename,foldername):
    df.write.format('delta').mode('append').option('path',gold_zone_path+foldername+'/'+filename)\
        .saveAsTable(goldDB+'.'+filename)
    df.write.format('delta').mode('append').save(gold_zone_path+filename)

# COMMAND ----------

#1. Total Flights: Total number of flights.
num_flights=df_routes.count()
df_num_flights=spark.createDataFrame([(num_flights,)],['num_of_flights'])
df_num_flights.show()
if loadType=='FL':
    createDeltaFL(df_num_flights,'num_flights','deltaTblQ1')
elif loadType=='IL':
    createDeltaIL(df_num_flights,'num_flights','deltaTblQ1')

# COMMAND ----------

#2. active airlines list.
df_active_airlines=df_airlines.select('Airline_ID','Name').where(col('Active')=='Y').orderBy('Airline_ID')
df_active_airlines.show()
if loadType=='FL':
    createDeltaFL(df_active_airlines,'active_airlines','deltaTblQ2')
elif loadType=='IL':
    createDeltaIL(df_active_airlines,'active_airlines','deltaTblQ2')

# COMMAND ----------

#3. how much distance the plane travels in each route.
from pyspark.sql.types import IntegerType, DoubleType
import math

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

df_airports_routes = df_routes\
    .join(df_airports,(df_airports.IATA == df_routes.Source_airport)|(df_airports.Airport_ID == df_routes.Source_airport_ID),'inner')\
    .select('Source_airport_ID','Source_airport','Name','Latitude','Longitude','Destination_airport','Destination_airport_ID')\
    .withColumnRenamed('Latitude','src_latitude')\
    .withColumnRenamed('Longitude','src_longitude')\
    .withColumnRenamed('Source_airport','Source_IATA')\
    .withColumnRenamed('Name','Source_Airport')
df_airports_routes = df_airports_routes\
    .join(df_airports,(df_airports.IATA == df_routes.Destination_airport)|(df_airports.Airport_ID == df_routes.Destination_airport_ID),'inner')\
    .select('Source_airport_ID','Source_IATA','Source_Airport','src_latitude','src_longitude','Destination_airport_ID','Destination_airport','Name','Latitude','Longitude')\
    .withColumnRenamed('Latitude','dst_latitude')\
    .withColumnRenamed('Longitude','dst_longitude')\
    .withColumnRenamed('Destination_airport','Destination_IATA')\
    .withColumnRenamed('Name','Destination_Airport')

data=[]
for row in df_airports_routes.collect():
    distance=haversine(row.src_latitude, row.src_longitude, row.dst_latitude, row.dst_longitude)
    data.append([row.Source_airport_ID,row.Source_IATA, row.Source_Airport, row.Destination_airport_ID,row.Destination_IATA,row.Destination_Airport, distance])
    
df_routes_distance=spark.createDataFrame(data,['Source_airport_ID','Source_IATA','Source_airport', 'Destination_airport_ID','Destination_IATA','Destination_airport','Distance_in_km'])
display(df_routes_distance)

if loadType=='FL':
    createDeltaFL(df_routes_distance,'routes_distance','deltaTblQ3')
elif loadType=='IL':
    createDeltaIL(df_routes_distance,'routes_distance','deltaTblQ3')

# COMMAND ----------

#4. airplanes operated by each airlines.
result_df = df_routes.groupBy("Airline")\
    .agg(
        first("Airline_ID").alias("Airline_ID"), 
        collect_set("Equipment").alias("Airplane_IATA")
    )\
    .select('Airline', 'Airline_ID', 'Airplane_IATA')
data=[]
count_data=[]
for row in result_df.collect():
    airline_ID=row.Airline_ID
    airline_IATA=row.Airline
    wordset=set()
    for words in row.Airplane_IATA:
        words=words.split('_')
        for word in words:   
            (word!='NA') and (wordset.add(word))
    record=(airline_ID,airline_IATA,list(wordset))
    count_record=(airline_ID,airline_IATA,len(wordset))
    data.append(record)
    count_data.append(count_record) 

# COMMAND ----------


df_airline_airplanes=spark.createDataFrame(data,['Airline_ID','Airline_IATA','Airplane_IATA'])
df_airline_airplanes=df_airline_airplanes\
    .join(df_airlines,df_airline_airplanes.Airline_ID==df_airlines.Airline_ID,'leftouter')\
    .select(df_airline_airplanes.Airline_ID,'Airline_IATA','Name','Airplane_IATA')\
    .withColumn('Name', when(df_airlines['Name'].isNull(), 'NA').otherwise(df_airlines['Name']))\
    .withColumnRenamed('Name','Airline')\
    .orderBy('Airline_ID')
display(df_airline_airplanes)

# COMMAND ----------

df_airline_num_airplanes=spark.createDataFrame(count_data,['Airline_ID','Airline_IATA','Num_of_Airplanes'])
df_airline_num_airplanes=df_airline_num_airplanes\
    .join(df_airlines,df_airline_num_airplanes.Airline_ID==df_airlines.Airline_ID,'leftouter')\
    .select(df_airline_num_airplanes.Airline_ID,'Airline_IATA','Name','Num_of_Airplanes')\
    .withColumn('Name', when(df_airlines['Name'].isNull(), 'NA').otherwise(df_airlines['Name']))\
    .withColumnRenamed('Name','Airline')\
    .orderBy('Airline_ID')
display(df_airline_num_airplanes)

if loadType=='FL':
    createDeltaFL(df_airline_num_airplanes,'airline_num_airplanes','deltaTblQ4')
elif loadType=='IL':
    createDeltaIL(df_airline_num_airplanes,'airline_num_airplanes','deltaTblQ4')

# COMMAND ----------

#5. country-wise number of airports 
df_countrywise_num_airports=df_airports.groupBy('Country').count().orderBy('count', ascending=False)\
    .withColumnRenamed('count','num_of_airports')
df_countrywise_num_airports.show()

if loadType=='FL':
    createDeltaFL(df_countrywise_num_airports,'countrywise_num_airports','deltaTblQ5')
elif loadType=='IL':
    createDeltaIL(df_countrywise_num_airports,'countrywise_num_airports','deltaTblQ5')

# COMMAND ----------

#6. country-wise airports list 
df_countrywise_airports=df_airports.groupBy("Country").agg(collect_set("Name").alias("Airports"))
df_countrywise_airports.show()

# COMMAND ----------

#7.number of routes linked with specific airports
df_routes_expanded = df_routes.select(
    col("Source_airport").alias("airport")
).union(
    df_routes.select(
        col("Destination_airport").alias("airport")
    )
)

# Group by airport and count the number of routes linked with each airport
df_num_routes_airport= df_routes_expanded.groupBy("airport").count()

# Show the result
display(df_num_routes_airport)

if loadType=='FL':
    createDeltaFL(df_num_routes_airport,'num_routes_airport','deltaTblQ7')
elif loadType=='IL':
    createDeltaIL(df_num_routes_airport,'num_routes_airport','deltaTblQ7')

# COMMAND ----------

#8. number of active airlines.
num_active_airlines=df_airlines.where(col("Active")=="Y").count()
df_num_active_airlines=spark.createDataFrame([(num_active_airlines,)],["Num_of_Active_Airlines"])
df_num_active_airlines.show()

if loadType=='FL':
    createDeltaFL(df_num_active_airlines,'num_active_airlines','deltaTblQ8')
elif loadType=='IL':
    createDeltaIL(df_num_active_airlines,'num_active_airlines','deltaTblQ8')

# COMMAND ----------

#9. routes with stops(>0).
df_routes_no_stop=df_routes.filter(col('Stops')>0)
display(df_routes_no_stop)
if loadType=='FL':
    createDeltaFL(df_routes_no_stop,'routes_no_stop','deltaTblQ9')
elif loadType=='IL':
    createDeltaIL(df_routes_no_stop,'routes_no_stop','deltaTblQ9')

# COMMAND ----------

#10. number of airplanes travelling in each route.
df = df_routes.groupBy('Source_airport', 'Source_airport_ID', 'Destination_airport', 'Destination_airport_ID') \
              .agg(collect_set("Equipment").alias("Equipment")).orderBy('Equipment')
count_data=[]
for row in df.collect():
    src_airport=row.Source_airport
    src_ID=row.Source_airport_ID
    dst_airport=row.Destination_airport
    dst_ID=row.Destination_airport_ID
    wordset=set()
    for words in row.Equipment:
        words=words.split('_')
        for word in words:   
            ((word!='NA')|(word!=' ')) and (wordset.add(word))
    count_record=(src_ID,src_airport,dst_ID,dst_airport,len(wordset))
    count_data.append(count_record) 

df_num_airplanes_route=spark.createDataFrame(count_data,['Source_airport_ID','Source_airport','Destination_airport_ID','Destination_airport','Num_of_Airplanes']).orderBy('Num_of_Airplanes')
display(df_num_airplanes_route)

if loadType=='FL':
    createDeltaFL(df_num_airplanes_route,'num_airplanes_route','deltaTblQ10')
elif loadType=='IL':
    createDeltaIL(df_num_airplanes_route,'num_airplanes_route','deltaTblQ10')

# COMMAND ----------

#11. callsign of airlines.
df_callsign_airlines=df_airlines.select('Airline_ID','Name','Callsign').where(col('Callsign')!='NA')
display(df_callsign_airlines)
if loadType=='FL':
    createDeltaFL(df_callsign_airlines,'callsign_airlines','deltaTblQ11')
elif loadType=='IL':
    createDeltaIL(df_callsign_airlines,'callsign_airlines','deltaTblQ11')

# COMMAND ----------

#12. country-wise number of airlines.
df_countrywise_num_airlines=df_airlines.groupBy('Country').count().orderBy('count',ascending=False)\
    .withColumnRenamed('count','num_of_airlines')
display(df_countrywise_num_airlines)
if loadType=='FL':
    createDeltaFL(df_countrywise_num_airlines,'countrywise_num_airlines','deltaTblQ12')
elif loadType=='IL':
    createDeltaIL(df_countrywise_num_airlines,'countrywise_num_airlines','deltaTblQ12')

# COMMAND ----------

#13. list of airlines with alias names.
df_alias_airlines=df_airlines.select('Airline_ID','Name','Alias').where(col('Alias')!='NA')
display(df_alias_airlines)
if loadType=='FL':
    createDeltaFL(df_alias_airlines,'alias_airlines','deltaTblQ13')
elif loadType=='IL':
    createDeltaIL(df_alias_airlines,'alias_airlines','deltaTblQ13')

# COMMAND ----------

#14. airports on high altitudes(>1524 meter or 5000feet)
df_high_altitude_airports=df_airports.select('Airport_ID','Name','City','Country','Altitude').where(col('Altitude')>1524)\
    .orderBy('Altitude',ascending=False)
display(df_high_altitude_airports)
if loadType=='FL':
    createDeltaFL(df_high_altitude_airports,'high_altitude_airports','deltaTblQ14')
elif loadType=='IL':
    createDeltaIL(df_high_altitude_airports,'high_altitude_airports','deltaTblQ14')

# COMMAND ----------

#15. airports using DST
df_dst_airport=df_airports.select('Airport_ID','Name','City','Country','DST').where(col('DST')!='NA')
display(df_dst_airport)
if loadType=='FL':
    createDeltaFL(df_dst_airport,'dst_airport','deltaTblQ15')
elif loadType=='IL':
    createDeltaIL(df_dst_airport,'dst_airport','deltaTblQ15')

# COMMAND ----------

#%sql
#drop database if exists db_global_airlines_system_gold cascade;