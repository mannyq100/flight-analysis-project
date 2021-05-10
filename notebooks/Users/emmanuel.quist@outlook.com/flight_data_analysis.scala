// Databricks notebook source
// DBTITLE 1,Get environment variables for connecting to snowflake
val dbUserName = dbutils.secrets.get("snowflakeSecrets", "userid")
val dbPassword = dbutils.secrets.get("snowflakeSecrets", "password")

val snowflakeURL = dbutils.widgets.get("snowflakeUrl")
val snowflakeDB = dbutils.widgets.get("snowflakeDBName")
val snowflakeWarehouse = dbutils.widgets.get("snowflakeWarehouse")
val snowflakeSchema = dbutils.widgets.get("snowflakeSchema")

val snowflakeConfig = Map(
  "sfUrl" -> snowflakeURL,
  "sfUser" -> dbUserName,
  "sfPassword" -> dbPassword,
  "sfDatabase" -> snowflakeDB,
  "sfWarehouse" -> snowflakeWarehouse,
  "sfSchema" -> snowflakeSchema
)

// COMMAND ----------

// DBTITLE 1,Get path to files
val filesBasePath = dbutils.widgets.get("fileBasePath")

// COMMAND ----------

// DBTITLE 1,Define schema for reading files 
import org.apache.spark.sql.types._

val airportsSchema = StructType(Array(
  StructField("IATA_CODE", StringType, true),
  StructField("AIRPORT", StringType, true),
  StructField("CITY", StringType, true),
  StructField("STATE", StringType, true),
  StructField("COUNTRY", StringType, true),
  StructField("LATITUDE", DoubleType, true),
  StructField("LONGITUDE", DoubleType, true)
))

val flightsSchema = StructType(Array(
  StructField("YEAR", IntegerType, true),
  StructField("MONTH", IntegerType, true),
  StructField("DAY", IntegerType, true),
  StructField("DAY_OF_WEEK", IntegerType, true),
  StructField("AIRLINE", StringType, true),
  StructField("FLIGHT_NUMBER", StringType, true),
  StructField("TAIL_NUMBER", StringType, true),
  StructField("ORIGIN_AIRPORT", StringType, true),
  StructField("DESTINATION_AIRPORT", StringType, true),
  StructField("SCHEDULED_DEPARTURE", IntegerType, true),
  StructField("DEPARTURE_TIME", StringType, true),
  StructField("DEPARTURE_DELAY", IntegerType, true),
  StructField("TAXI_OUT", IntegerType, true),
  StructField("WHEELS_OFF", StringType, true),
  StructField("SCHEDULED_TIME", IntegerType, true),
  StructField("ELAPSED_TIME", IntegerType, true),
  StructField("AIR_TIME", IntegerType, true),
  StructField("DISTANCE", IntegerType, true),
  StructField("WHEELS_ON", IntegerType, true),
  StructField("TAXI_IN", IntegerType, true),
  StructField("SCHEDULED_ARRIVAL", IntegerType, true),
  StructField("ARRIVAL_TIME", StringType, true),
  StructField("ARRIVAL_DELAY", StringType, true),
  StructField("DIVERTED", IntegerType, true),
  StructField("CANCELLED", IntegerType, true),
  StructField("CANCELLATION_REASON", StringType, true),
  StructField("AIR_SYSTEM_DELAY", IntegerType, true),
  StructField("SECURITY_DELAY", IntegerType, true),
  StructField("AIRLINE_DELAY", IntegerType, true),
  StructField("LATE_AIRCRAFT_DELAY", IntegerType, true),
  StructField("WEATHER_DELAY", IntegerType, true)
))

// COMMAND ----------

// DBTITLE 1,Read the files into a dataframe

val airlinesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filesBasePath+"airlines.csv")
val airportsDF = spark.read.format("csv").option("header", "true").schema(airportsSchema).load(filesBasePath+"airports.csv")
val flightsDF = spark.read.format("csv").option("header", "true").schema(flightsSchema).load(filesBasePath+"flights")


// COMMAND ----------

// DBTITLE 1,Write the dataframes to respective tables in snowflake
import org.apache.spark.sql.SaveMode

airlinesDF.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRLINES").mode(SaveMode.Overwrite).save()
airportsDF.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRPORTS").mode(SaveMode.Overwrite).save()
flightsDF.write.format("snowflake").options(snowflakeConfig).option("dbtable", "FLIGHTS").mode(SaveMode.Overwrite).save()



// COMMAND ----------

// MAGIC %md Created temp tables in spark for each file. This will be used to create views that will be written  to tables in snowflake. This approach is used because snowflake free edition does not support materialized views. Using just regular views is expensive 

// COMMAND ----------

airlinesDF.createOrReplaceTempView("airlines")
airportsDF.createOrReplaceTempView("airports")
flightsDF.createOrReplaceTempView("flights")

// COMMAND ----------

// DBTITLE 1,Report for total flights for airline per airport on monthly basis
val flightsByAirlineAndAirport = spark.sql(s"""
SELECT A.AIRLINE, COUNT(*) AS TOTAL_NUM_FLIGHTS, AP.AIRPORT, F.MONTH
FROM FLIGHTS F JOIN AIRLINES A ON F.AIRLINE = A.IATA_CODE
JOIN AIRPORTS AP ON F.ORIGIN_AIRPORT = AP.IATA_CODE
GROUP BY A.AIRLINE, AP.AIRPORT, F.MONTH
ORDER BY TOTAL_NUM_FLIGHTS DESC
""")

flightsByAirlineAndAirport.write.format("snowflake").options(snowflakeConfig).option("dbtable", "TOTAL_FLIGHTS_BY_AIRLINE_MONTHLY").mode(SaveMode.Overwrite).save()

display(flightsByAirlineAndAirport)

// COMMAND ----------

// DBTITLE 1,Report for on-time percentage for each airline
import net.snowflake.spark.snowflake.Utils

var onTimePercentageForAirline = spark.sql(s"""
SELECT TABLE2.AIRLINE, TABLE2.IATA_CODE, TABLE2.YEAR,TOTAL_FLIGHTS, TOTAL_ONTIME,  ROUND((TOTAL_ONTIME/TOTAL_FLIGHTS)*100) AS PERECTAGE_ON_TIME
FROM (
    SELECT COUNT(*) AS TOTAL_ONTIME, AIRLINE, YEAR
    FROM FLIGHTS F 
    WHERE DEPARTURE_DELAY <= 0
    GROUP BY AIRLINE, YEAR) AS TABLE1
JOIN (
    SELECT YEAR, COUNT(*) AS TOTAL_FLIGHTS, F.AIRLINE AS IATA_CODE, A.AIRLINE
    FROM FLIGHTS F JOIN AIRLINES A
    ON F.AIRLINE = A.IATA_CODE
    GROUP BY YEAR, F.AIRLINE, A.AIRLINE) AS TABLE2
WHERE TABLE1.YEAR = TABLE2.YEAR 
  AND TABLE1.AIRLINE = TABLE2.IATA_CODE
  ORDER BY PERECTAGE_ON_TIME DESC;
""")

onTimePercentageForAirline.write.format("snowflake").options(snowflakeConfig).option("dbtable", "PERCENTAGE_FLIGHTS_ON_TIME").mode(SaveMode.Overwrite).save()

display(onTimePercentageForAirline)
//Utils.runQuery(snowflakeConfig, """SELECT * FROM PERCENTAGE_FLIGHTS_ON_TIME""")

// COMMAND ----------

// DBTITLE 1,Report for airlines with largest number of delays

var airlineDelays = spark.sql(s"""
SELECT   A.AIRLINE, COUNT(*) AS TOTAL_DELAYS
FROM FLIGHTS F JOIN AIRLINES A
ON F.AIRLINE = A.IATA_CODE
WHERE F.DEPARTURE_DELAY > 0
GROUP BY  A.AIRLINE
ORDER BY 2 DESC;
""")
airlineDelays.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRLINE_DELAYS_COUNT").mode(SaveMode.Overwrite).save()

display(airlineDelays)

// COMMAND ----------

// DBTITLE 1,Report for cancellations reasons by airport
val airportCancelationReasons = spark.sql(s"""
SELECT A.AIRPORT, A.IATA_CODE, 
  CASE
    WHEN F.CANCELLATION_REASON = 'A' THEN 'Airline/Carrier'
    WHEN F.CANCELLATION_REASON = 'B' THEN 'Weather'
    WHEN F.CANCELLATION_REASON = 'C' THEN 'National Air System'
    WHEN F.CANCELLATION_REASON = 'D' THEN 'Security'
    ELSE 'Not Provided'
  END AS CANCELLATION_REASON,
COUNT(*) AS CANCEL_REASON_COUNT
 FROM FLIGHTS F JOIN AIRPORTS A
ON F.ORIGIN_AIRPORT = A.IATA_CODE
GROUP BY A.AIRPORT, CANCELLATION_REASON, A.IATA_CODE
""")

airportCancelationReasons.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRPORT_CANCELATION_REASONS").mode(SaveMode.Overwrite).save()

val cancellationReasons = spark.read.format("snowflake") 
                          .options(snowflakeConfig) 
                          .option("query", "SELECT * FROM AIRPORT_CANCELATION_REASONS WHERE NOT CANCELLATION_REASON = 'Not Provided' ") 
                          .load()


display(cancellationReasons)

// COMMAND ----------

// DBTITLE 1,Report for delay reasons by airport
val airportDelayReasons = spark.sql(s"""
SELECT A.AIRPORT, SUM(F.AIR_SYSTEM_DELAY) AS TOTAL_AIR_SYSTEM_DETALY_TIME, SUM(F.SECURITY_DELAY) AS TOTAL_SECURITY_DELAY_TIME, 
SUM(F.AIRLINE_DELAY) AS TOTAL_AIRLINE_DELAY_TIME, SUM(F.LATE_AIRCRAFT_DELAY) AS TOTAL_AIRCRAFT_DELAY_TIME, SUM(F.WEATHER_DELAY) AS TOTAL_WEATHER_DELAY_TIME
FROM FLIGHTS F JOIN AIRPORTS A
ON F.ORIGIN_AIRPORT = A.IATA_CODE
GROUP BY A.AIRPORT
""")

airportDelayReasons.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRPORT_DELAY_REASONS").mode(SaveMode.Overwrite).save()

display(airportDelayReasons)

// COMMAND ----------

// DBTITLE 1,Report for airlines with the most unique routes
val airlineRoutes = spark.sql(s"""
SELECT DISTINCT A.AIRLINE, CONCAT(F.ORIGIN_AIRPORT, '-', F.DESTINATION_AIRPORT) AS ROUTE 
FROM FLIGHTS F JOIN AIRLINES A 
ON F.AIRLINE = A.IATA_CODE 
""")

airlineRoutes.write.format("snowflake").options(snowflakeConfig).option("dbtable", "AIRLINE_ROUTES").mode(SaveMode.Overwrite).save()

val airlineUniqueRoutes = spark.read.format("snowflake") 
                          .options(snowflakeConfig) 
                          .option("query", "SELECT AIRLINE,  COUNT(ROUTE) AS UNIQUE_ROUTES FROM AIRLINE_ROUTES GROUP BY AIRLINE ORDER BY UNIQUE_ROUTES") 
                          .load()

display(airlineUniqueRoutes)


// COMMAND ----------

