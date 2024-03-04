#open hive

hive

#to show dbs

show databases;


#use default db
use default;


#create an external table 

CREATE EXTERNAL TABLE IF NOT EXISTS flight_data(
  Counter int,
  Year int, 
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/user/tables/flight_data';

#show the external table

show tables;

#load the csv data from s3
LOAD DATA INPATH 's3://my-map-reduce-cluster/data/DelayedFlights-updated.csv' OVERWRITE INTO TABLE flight_data;

#print records with header
	
SET hive.cli.print.header=true;


#CarrierDelay Query

SELECT Year As Year, avg((CarrierDelay /ArrDelay)*100) As AvgCarrierDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;





#NASDelay Query

SELECT Year As Year, avg((NASDelay /ArrDelay)*100) As AvgNASDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;


#WeatherDelay Query

SELECT Year As Year, avg((WeatherDelay /ArrDelay)*100) As AvgWeatherDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;


#LateAircraftDelay Query

SELECT Year As Year, avg((LateAircraftDelay /ArrDelay)*100) As AvgLateAircraftDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;


#SecurityDelay Query

SELECT Year As Year, avg((SecurityDelay /ArrDelay)*100) As AvgSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;


