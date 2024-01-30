# PySpark Indian Flights Analysis



```python
# Importing PySpark library for distributed data processing
import pyspark as sp
# Creating a SparkContext to initialize Spark
sc = sp.SparkContext.getOrCreate()
# Displaying the SparkContext information
print("SparkContext:", sc)
# Printing the Spark version for reference
print("Spark Version:", sc.version)
```

    SparkContext: <SparkContext master=local[*] appName=pyspark-shell>
    Spark Version: 3.5.0



```python
# Importing the SparkSession module from PySpark for working with structured data
from pyspark.sql import SparkSession

# Creating or getting an existing Spark session
spark = SparkSession.builder.getOrCreate()

# Displaying the SparkSession information
print("SparkSession:", spark)
```

    SparkSession: <pyspark.sql.session.SparkSession object at 0x7f70cd799db0>



```python
# Importing necessary libraries for data manipulation and analysis
import pandas as pd
import numpy as np

# Generating a Pandas DataFrame with random data
pd_temp = pd.DataFrame(np.random.random(10))

# Converting the Pandas DataFrame to a PySpark DataFrame
spark_temp = spark.createDataFrame(pd_temp)

# Displaying the list of tables in the Spark catalog before and after creating a temporary view
print("Tables in Spark Catalog before creating temporary view:", spark.catalog.listTables())

# Creating a temporary view named "temp" for the PySpark DataFrame
spark_temp.createOrReplaceTempView("temp")

# Displaying the list of tables in the Spark catalog after creating a temporary view
print("Tables in Spark Catalog after creating temporary view:", spark.catalog.listTables())

```

    Tables in Spark Catalog before creating temporary view: [Table(name='temp', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]
    Tables in Spark Catalog after creating temporary view: [Table(name='temp', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]



```python
# Defining the file path for the Indian flights data
file_path = './data/Flight_Schedule.csv'

# Reading the data from the CSV file into a PySpark DataFrame
flights = spark.read.csv(file_path, header=True)

# Displaying a preview of the loaded data
flights.show()

```

    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |airline|flightNumber|   origin|destination|          daysOfWeek|scheduledDepartureTime|scheduledArrivalTime|  timezone| validFrom|   validTo|lastUpdated|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |  GoAir|         425|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 05:45|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|            Saturday|                 07:30|                  NA|2018-10-28|2018-10-28|2018-10-28| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2018-12-01|2018-11-03|2018-12-01| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2019-03-30|2019-02-02|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2018-11-30|2018-10-29|2018-11-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-03-29|2019-02-01|2019-03-29| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-01-31|2018-12-02|2019-01-31| 2023-11-05|
    |  GoAir|         422|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 20:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         559|  Lucknow|  Hyderabad|Sunday,Tuesday,We...|                 15:45|               17:45|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         580|    Kochi|  Hyderabad|Sunday,Monday,Tue...|                 04:45|               06:15|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|Sunday,Tuesday,We...|                 17:20|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|              Monday|                 17:50|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:05|               12:45|2019-03-30|2018-10-29|2019-03-30| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:35|               22:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:00|               12:45|2019-10-26|2019-04-01|2019-10-26| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:40|               22:20|2019-10-26|2019-03-31|2019-10-26| 2023-11-05|
    |  GoAir|         701|   Jaipur|  Hyderabad|            Saturday|                 08:55|               10:55|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    |  GoAir|         431|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 12:45|               15:05|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         385|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 17:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         894|Bengaluru|  Hyderabad|            Saturday|                 11:30|               12:40|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    only showing top 20 rows
    



```python
# Checking the data type of the 'flights' DataFrame
type(flights)
```




    pyspark.sql.dataframe.DataFrame




```python
# Displaying the list of available databases in the Spark catalog
spark.catalog.listDatabases()
```




    [Database(name='default', catalog='spark_catalog', description='default database', locationUri='file:/mnt/d/TST14/pyspark-indian-flights-analysis/spark-warehouse')]




```python
# Displaying the list of tables in the Spark catalog
spark.catalog.listTables()
```




    [Table(name='temp', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]




```python
# Creating a temporary view named 'flights' for the flight schedule DataFrame
flights.name = flights.createOrReplaceTempView('flights')

# Displaying the list of tables in the Spark catalog after creating a temporary view
spark.catalog.listTables()
```




    [Table(name='flights', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True),
     Table(name='temp', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]




```python
# Retrieving the flight schedule DataFrame from the temporary view 'flights'
flights_df = spark.table('flights')

# Displaying the first few rows of the flight schedule DataFrame
print(flights_df.show())
```

    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |airline|flightNumber|   origin|destination|          daysOfWeek|scheduledDepartureTime|scheduledArrivalTime|  timezone| validFrom|   validTo|lastUpdated|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |  GoAir|         425|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 05:45|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|            Saturday|                 07:30|                  NA|2018-10-28|2018-10-28|2018-10-28| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2018-12-01|2018-11-03|2018-12-01| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2019-03-30|2019-02-02|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2018-11-30|2018-10-29|2018-11-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-03-29|2019-02-01|2019-03-29| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-01-31|2018-12-02|2019-01-31| 2023-11-05|
    |  GoAir|         422|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 20:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         559|  Lucknow|  Hyderabad|Sunday,Tuesday,We...|                 15:45|               17:45|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         580|    Kochi|  Hyderabad|Sunday,Monday,Tue...|                 04:45|               06:15|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|Sunday,Tuesday,We...|                 17:20|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|              Monday|                 17:50|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:05|               12:45|2019-03-30|2018-10-29|2019-03-30| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:35|               22:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:00|               12:45|2019-10-26|2019-04-01|2019-10-26| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:40|               22:20|2019-10-26|2019-03-31|2019-10-26| 2023-11-05|
    |  GoAir|         701|   Jaipur|  Hyderabad|            Saturday|                 08:55|               10:55|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    |  GoAir|         431|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 12:45|               15:05|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         385|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 17:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         894|Bengaluru|  Hyderabad|            Saturday|                 11:30|               12:40|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    only showing top 20 rows
    
    None


# 1. Flight Counts by Airline:


```python
# Clean data for Airline Counts analysis
# Replace 'NA' strings with actual None values for Airline Counts analysis
airline_counts_data = flights_df.na.replace('NA', None, subset=['airline']).na.drop(subset=['airline'])

# Perform the Flight Counts by Airline analysis
airline_counts = airline_counts_data.groupBy('airline').count().orderBy('count', ascending=False)

```


```python
airline_counts.show()
```

    +--------------------+-----+
    |             airline|count|
    +--------------------+-----+
    |              IndiGo|41291|
    |               GoAir|11666|
    |            SpiceJet|10824|
    |           Air India| 8185|
    |       AirAsia India| 5227|
    |             Vistara| 4332|
    |Alliance Air (India)| 3069|
    |         Jet Airways| 1307|
    |              TruJet| 1141|
    |            Star Air|  472|
    |           Akasa Air|  438|
    |              FlyBig|  214|
    |             Jetlite|  117|
    |        IndiaOne Air|   24|
    +--------------------+-----+
    


# 2. Flight Counts by Origin-Destination Pair:


```python
# Clean data for Origin-Destination Counts analysis
origin_dest_counts_data = flights_df.na.replace('NA', None, subset=['origin', 'destination']).na.drop(subset=['origin', 'destination'])
origin_dest_counts = airline_counts_data.groupBy('origin', 'destination').count().orderBy('count', ascending=False)
origin_dest_counts.show()

```

    [Stage 25:=============================>                            (2 + 2) / 4]

    +---------+-----------+-----+
    |   origin|destination|count|
    +---------+-----------+-----+
    |   Mumbai|      Delhi| 1339|
    |    Delhi|     Mumbai| 1297|
    |Bengaluru|      Delhi| 1018|
    | Srinagar|      Delhi|  973|
    |    Delhi|  Bengaluru|  924|
    |    Delhi|   Srinagar|  913|
    |    Delhi|    Kolkata|  802|
    |  Kolkata|      Delhi|  784|
    |   Mumbai|  Bengaluru|  768|
    |Bengaluru|     Mumbai|  737|
    |Bengaluru|  Hyderabad|  649|
    |  Kolkata|  Bengaluru|  607|
    |Ahmedabad|      Delhi|  605|
    |     Pune|      Delhi|  594|
    |    Delhi|       Pune|  593|
    |Hyderabad|  Bengaluru|  557|
    |    Delhi|  Hyderabad|  542|
    |    Delhi|  Ahmedabad|  539|
    |Bengaluru|    Kolkata|  537|
    |Hyderabad|      Delhi|  534|
    +---------+-----------+-----+
    only showing top 20 rows
    


                                                                                    

# 3. Average Departure and Arrival Times:


```python
from pyspark.sql.functions import avg,col, unix_timestamp, from_unixtime
```


```python
# Replace 'NA' with None and drop null values
avg_times_data = flights_df.na.replace('NA', None, subset=['scheduledDepartureTime', 'scheduledArrivalTime']).na.drop(subset=['scheduledDepartureTime', 'scheduledArrivalTime'])
```


```python
# Convert 'scheduledDepartureTime' to seconds since midnight and filter out null values
avg_times_data = avg_times_data.withColumn('scheduledDepartureTime', unix_timestamp(col('scheduledDepartureTime'), 'HH:mm').cast('int'))
avg_times_data = avg_times_data.filter(avg_times_data['scheduledDepartureTime'].isNotNull())

# Convert 'scheduledArrivalTime' to seconds since midnight and filter out null values
avg_times_data = avg_times_data.withColumn('scheduledArrivalTime', unix_timestamp(col('scheduledArrivalTime'), 'HH:mm').cast('int'))
avg_times_data = avg_times_data.filter(avg_times_data['scheduledArrivalTime'].isNotNull())
```


```python
# Calculate average departure and arrival times grouped by origin
avg_departure_time = avg_times_data.groupBy('origin').agg(avg('scheduledDepartureTime').alias('avg_departure_time'))
avg_arrival_time = avg_times_data.groupBy('origin').agg(avg('scheduledArrivalTime').alias('avg_arrival_time'))

# Convert average departure and arrival times back to 'HH:mm' format
avg_departure_time = avg_departure_time.withColumn('avg_departure_time', from_unixtime('avg_departure_time', 'HH:mm'))
avg_arrival_time = avg_arrival_time.withColumn('avg_arrival_time', from_unixtime('avg_arrival_time', 'HH:mm'))
```


```python
# Join the results on the 'origin' column
result = avg_departure_time.join(avg_arrival_time, on='origin')

# Show the results
result.show()
```

    [Stage 28:=============>    (3 + 1) / 4][Stage 29:=========>        (2 + 2) / 4]

    +--------------+------------------+----------------+
    |        origin|avg_departure_time|avg_arrival_time|
    +--------------+------------------+----------------+
    |       Udaipur|             14:48|           16:22|
    |       Dimapur|             12:26|           13:48|
    |         Kochi|             12:44|           13:54|
    |    Aurangabad|             14:58|           16:28|
    |   Bhubaneswar|             13:46|           15:21|
    |        Aizwal|             13:22|           14:24|
    |        Mysore|             15:10|           16:29|
    |         MIHAN|             12:32|           13:56|
    |         Jammu|             14:17|           15:28|
    |       Jalgaon|             14:59|           16:21|
    |      Agartala|             14:28|           15:37|
    |          Bhuj|             12:12|           13:47|
    |     Darbhanga|             13:47|           15:58|
    |         Hubli|             13:56|           15:19|
    |         Patna|             14:23|           16:05|
    |Shirdi Airport|             13:57|           15:40|
    |       Jodhpur|             14:29|           16:09|
    |        Tezpur|             12:44|           13:29|
    |      Vadodara|             14:02|           15:36|
    |     Jaisalmer|             13:05|           14:35|
    +--------------+------------------+----------------+
    only showing top 20 rows
    


                                                                                    

# 4. Busiest Day of the Week:


```python
from pyspark.sql.functions import explode, split
```


```python
# Clean data for Busiest Day of the Week analysis
day_counts_data = flights_df.na.replace('NA', None, subset=['daysOfWeek']).na.drop(subset=['daysOfWeek'])

```


```python
day_counts = (
    day_counts_data
    .select('daysOfWeek')
    .withColumn('day', explode(split('daysOfWeek', ',')))
    .groupBy('day')
    .count()
    .orderBy('count', ascending=False)
)
```


```python
day_counts.show()
```

    +---------+-----+
    |      day|count|
    +---------+-----+
    | Saturday|61171|
    |   Friday|59906|
    |  Tuesday|58477|
    | Thursday|58327|
    |   Monday|57993|
    |Wednesday|57902|
    |   Sunday|57691|
    +---------+-----+
    


# 5. Busiest Airports and Airlines


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import matplotlib.pyplot as plt
```


```python
# Identify busiest airports by counting the number of departures and arrivals
busiest_airports = flights_df.groupBy("origin").agg(count("*").alias("total_flights")).orderBy("total_flights", ascending=False)
```


```python
busiest_airports.show()
```

    [Stage 37:==============>                                           (1 + 3) / 4]

    +-----------+-------------+
    |     origin|total_flights|
    +-----------+-------------+
    |      Delhi|        13332|
    |  Bengaluru|         8941|
    |     Mumbai|         8301|
    |    Kolkata|         6120|
    |  Hyderabad|         5678|
    |    Chennai|         4824|
    |  Ahmedabad|         3560|
    |       Pune|         2726|
    |        Goa|         2251|
    |   Srinagar|         1897|
    |     Jaipur|         1615|
    |    Lucknow|         1507|
    |   Guwahati|         1430|
    | Chandigarh|         1421|
    |      Kochi|         1258|
    |      Patna|         1187|
    | Port Blair|         1090|
    |Bhubaneswar|         1085|
    |     Indore|         1022|
    |      MIHAN|          967|
    +-----------+-------------+
    only showing top 20 rows
    


                                                                                    


```python
# Plotting TOP !0 busiest airports

busiest_airports_plot = busiest_airports.toPandas()
# Sorting the DataFrame by total_flights in descending order
busiest_airports_plot = busiest_airports_plot.sort_values(by='total_flights', ascending=False)

# Selecting only the top 10 busiest airports
top_10_airports = busiest_airports_plot.head(10)

# Plotting the top 10 busiest airports
plt.figure(figsize=(12, 6))
top_10_airports.plot(kind='bar', x='origin', y='total_flights', color='skyblue', legend=False)
plt.title('Top 10 Busiest Airports')
plt.xlabel('Airport')
plt.ylabel('Total Flights')
plt.xticks(rotation=45, ha='right')
plt.show()

```


    <Figure size 1200x600 with 0 Axes>



    
![png](indian_flights_files/indian_flights_30_1.png)
    



```python
# Identify busiest airlines by counting the number of flights operated
# Filter out rows where airline is 'NA'
busiest_airlines = flights.filter(col("airline") != 'NA') \
                          .groupBy("airline") \
                          .agg(count("*").alias("total_flights")) \
                          .orderBy("total_flights", ascending=False)
```


```python
busiest_airlines.show()
```

    +--------------------+-------------+
    |             airline|total_flights|
    +--------------------+-------------+
    |              IndiGo|        41291|
    |               GoAir|        11666|
    |            SpiceJet|        10824|
    |           Air India|         8185|
    |       AirAsia India|         5227|
    |             Vistara|         4332|
    |Alliance Air (India)|         3069|
    |         Jet Airways|         1307|
    |              TruJet|         1141|
    |            Star Air|          472|
    |           Akasa Air|          438|
    |              FlyBig|          214|
    |             Jetlite|          117|
    |        IndiaOne Air|           24|
    +--------------------+-------------+
    



```python
# Plotting busiest airlines
busiest_airlines_plot = busiest_airlines.toPandas()
# Sorting the DataFrame by total_flights in descending order
busiest_airlines_plot = busiest_airlines_plot.sort_values(by='total_flights', ascending=False)

# Plotting busiest airlines as a horizontal bar chart
plt.figure(figsize=(12, 6))
plt.barh(busiest_airlines_plot['airline'], busiest_airlines_plot['total_flights'], color='lightcoral')
plt.title('Busiest Airlines')
plt.xlabel('Total Flights')
plt.ylabel('Airline')
plt.show()


```


    
![png](indian_flights_files/indian_flights_33_0.png)
    


# 6. Air Traffic Plots for Indian Airports


```python
import matplotlib.pyplot as plt
from pyspark.sql.functions import unix_timestamp, col, hour
from datetime import datetime
```


```python
flights_df.show()
```

    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |airline|flightNumber|   origin|destination|          daysOfWeek|scheduledDepartureTime|scheduledArrivalTime|  timezone| validFrom|   validTo|lastUpdated|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    |  GoAir|         425|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 05:45|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|            Saturday|                 07:30|                  NA|2018-10-28|2018-10-28|2018-10-28| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2018-12-01|2018-11-03|2018-12-01| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|              Friday|                 07:30|                  NA|2019-03-30|2019-02-02|2019-03-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2018-11-30|2018-10-29|2018-11-30| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-03-29|2019-02-01|2019-03-29| 2023-11-05|
    |  GoAir|         423|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 07:30|                  NA|2019-01-31|2018-12-02|2019-01-31| 2023-11-05|
    |  GoAir|         422|    Delhi|  Hyderabad|Sunday,Monday,Tue...|                 20:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         559|  Lucknow|  Hyderabad|Sunday,Tuesday,We...|                 15:45|               17:45|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         580|    Kochi|  Hyderabad|Sunday,Monday,Tue...|                 04:45|               06:15|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|Sunday,Tuesday,We...|                 17:20|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         587|    Kochi|  Hyderabad|              Monday|                 17:50|               19:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:05|               12:45|2019-03-30|2018-10-29|2019-03-30| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:35|               22:20|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         553|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 11:00|               12:45|2019-10-26|2019-04-01|2019-10-26| 2023-11-05|
    |  GoAir|         552|Ahmedabad|  Hyderabad|Sunday,Monday,Tue...|                 20:40|               22:20|2019-10-26|2019-03-31|2019-10-26| 2023-11-05|
    |  GoAir|         701|   Jaipur|  Hyderabad|            Saturday|                 08:55|               10:55|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    |  GoAir|         431|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 12:45|               15:05|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         385|   Jaipur|  Hyderabad|Sunday,Monday,Tue...|                 17:55|                  NA|2019-03-30|2018-10-28|2019-03-30| 2023-11-05|
    |  GoAir|         894|Bengaluru|  Hyderabad|            Saturday|                 11:30|               12:40|2019-03-24|2018-10-28|2019-03-24| 2023-11-05|
    +-------+------------+---------+-----------+--------------------+----------------------+--------------------+----------+----------+----------+-----------+
    only showing top 20 rows
    



```python
# Replace 'NA' with None and drop null values
flights_df_cleaned_times = flights_df.na.replace('NA', None, subset=['scheduledDepartureTime', 'scheduledArrivalTime']).na.drop(subset=['scheduledDepartureTime', 'scheduledArrivalTime'])
```


```python
# Convert 'scheduledDepartureTime' and 'scheduledArrivalTime' to seconds since midnight and filter out null values
flights_df_cleaned_times = flights_df_cleaned_times.withColumn('scheduledDepartureTime', unix_timestamp(col('scheduledDepartureTime'), 'HH:mm').cast('int'))
flights_df_cleaned_times = flights_df_cleaned_times.filter(flights_df_cleaned_times['scheduledDepartureTime'].isNotNull())
flights_df_cleaned_times = flights_df_cleaned_times.withColumn('scheduledArrivalTime', unix_timestamp(col('scheduledArrivalTime'), 'HH:mm').cast('int'))
flights_df_cleaned_times = flights_df_cleaned_times.filter(flights_df_cleaned_times['scheduledArrivalTime'].isNotNull())
```


```python
# Extract the hour of scheduled departure and arrival
flights_df_cleaned_times = flights_df_cleaned_times.withColumn('departureHour', hour(from_unixtime('scheduledDepartureTime')))
flights_df_cleaned_times = flights_df_cleaned_times.withColumn('arrivalHour', hour(from_unixtime('scheduledArrivalTime')))
```


```python
# Plotting the data
departure_data = flights_df_cleaned_times.groupBy('departureHour').count().orderBy('departureHour').toPandas()
arrival_data = flights_df_cleaned_times.groupBy('arrivalHour').count().orderBy('arrivalHour').toPandas()
```


```python
plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
plt.bar(departure_data['departureHour'], departure_data['count'])
plt.title('Scheduled Departures in Indian Airports')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Flights')
plt.xticks(range(0, 24, 4))
plt.tight_layout()
plt.show()

```


    
![png](indian_flights_files/indian_flights_41_0.png)
    



```python
plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 2)
plt.bar(arrival_data['arrivalHour'], arrival_data['count'])
plt.title('Scheduled Arrivals in Indian Airports')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Flights')
plt.xticks(range(0, 24, 4))
plt.tight_layout()
plt.show()
```


    
![png](indian_flights_files/indian_flights_42_0.png)
    

