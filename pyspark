# open pyspark

pyspark

# import

from pyspark.sql import SparkSession
import time

# create session

spark = SparkSession.builder.appName("DelayedFlightsData").getOrCreate()

# read the file


delayed_flights_data = spark.read.format("csv").option("header", "true").load("s3://my-spark-cluster/data/DelayedFlights-updated.csv")

delayed_flights_data.createOrReplaceTempView("delayed_flights_data")


#CarrierDelay Query

start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((CarrierDelay /ArrDelay)*100) As AvgCarrierDelay
	FROM delayed_flights_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")



#NASDelay Query

start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((NASDelay /ArrDelay)*100) As AvgNASDelay
	FROM delayed_flights_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")




#WeatherDelay Query

start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((WeatherDelay /ArrDelay)*100) As AvgWeatherDelay
	FROM delayed_flights_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")


#LateAircraftDelay Query

start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((LateAircraftDelay /ArrDelay)*100) As AvgLateAircraftDelay
	FROM delayed_flights_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")


#SecurityDelay Query

start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((SecurityDelay /ArrDelay)*100) As AvgSecurityDelay
	FROM delayed_flights_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")

