# NYC Rideshare Data Analysis

In the Coursework, you will apply Spark techniques to the NYC Rideshare dataset, which focuses on analyzing the New York 'Uber/Lyft' data from January 1, 2023, to May 31, 2023. Source data pre-processed was provided by the NYC Taxi and Limousine Commission (TLC) Trip Record Data hosted by the state of New York. The dataset used in the Coursework is distributed under the MIT license. The source of the datasets is available on this link:

https://www.kaggle.com/datasets/aaronweymouth/nyc-rideshare-raw-data?resource=download

Useful Resources: Lectures, Labs, and other materials are shared along with the following links:

https://sparkbyexamples.com/pyspark

https://sparkbyexamples.com/pyspark-tutorial/

https://spark.apache.org/docs/3.1.2/api/python/getting_started/index.html

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

You can see two CSV files under the path //data-repository-bkt/ECS765/rideshare_2023/, including rideshare_data.csv and taxi_zone_lookup.csv. Using the ccc method bucket ls command to check them.

## Dataset Schema
The information on the two CSV files is described below. Please read them carefully because it will help you understand the dataset and tasks.

### rideshare_data.csv
The data information for the rideshare_data.csv is shown below ( s means second ):

| Field               | Type      | Description                                                                                              |
|---------------------|-----------|----------------------------------------------------------------------------------------------------------|
| business            | string    | Uber and Lyft.                                                                                           |
| pickup_location     | string    | Taxi Zone where the journey commenced. Refer to 'taxi_zone_lookup.csv' for details.                     |
| dropoff_location    | string    | Taxi Zone where the journey concluded. Refer to 'taxi_zone_lookup.csv' for details.                     |
| trip_length         | string    | The total distance of the trip in miles.                                                                 |
| request_to_pickup   | string (s)| The time taken from the ride request to the passenger pickup.                                            |
| total_ride_time     | string (s)| The duration between the passenger pickup and dropoff, indicating the total time spent in the car.     |
| on_scene_to_pickup  | string (s)| The time duration between the driver's arrival on the scene and the passenger pickup, reflecting wait time. |
| on_scene_to_dropoff | string (s)| Time from the driver's arrival on the scene to the passenger dropoff, indicating the driver's total time commitment. |
| time_of_day         | string    | Categorization of the time of day: morning (0600-1100), afternoon (1200-1600), evening (1700-1900), night (other times). |
| date                | string (s)| The date when the ride was requested, expressed in UNIX timestamp.                                       |
| passenger_fare      | string    | The total fare paid by the passenger in USD, inclusive of all charges.                                   |
| driver_total_pay    | string    | The complete payment received by the driver, including base pay and tips.                                |
| rideshare_profit    | string    | The difference between the passenger fare and the driver's total pay, representing the platform's profit.|
| hourly_rate         | string    | The calculated hourly rate based on 'on_scene_hours', including the duration from arrival to final drop-off. |
| dollars_per_mile    | string    | The driver's earnings per mile, calculated as total pay divided by trip length.                          |

The table below shows the samples from the rideshare_data.csv:

| business   |   pickup_location |   dropoff_location |   trip_length |   request_to_pickup |   total_ride_time |   on_scene_to_pickup |   on_scene_to_dropoff | time_of_day   |       date |   passenger_fare |   driver_total_pay |   rideshare_profit |   hourly_rate |   dollars_per_mile |
|:-----------|------------------:|-------------------:|--------------:|--------------------:|------------------:|---------------------:|----------------------:|:--------------|-----------:|-----------------:|-------------------:|-------------------:|--------------:|-------------------:|
| Uber       |               102 |                157 |          2.08 |                 119 |               648 |                   79 |                   727 | evening       | 1684713600 |            12.61 |               9.1  |               3.51 |         45.06 |               4.38 |
| Uber       |               235 |                244 |          1.7  |                  59 |              1226 |                   27 |                  1253 | evening       | 1684713600 |            13.57 |              13.75 |              -0.18 |         39.51 |               8.09 |
| Uber       |                79 |                170 |          2.02 |                 438 |               835 |                   31 |                   866 | morning       | 1684800000 |            26.26 |              21.14 |               5.12 |         87.88 |              10.47 |
| Uber       |               256 |                196 |          6.27 |                 180 |              1362 |                   26 |                  1388 | morning       | 1684713600 |            31.21 |              25.04 |              10.17 |         64.95 |               3.99 |
| Uber       |               230 |                 43 |          1.63 |                  72 |               637 |                   65 |                   702 | morning       | 1684713600 |            24.44 |              13.13 |              16.31 |         67.33 |               8.06 |

### taxi_zone_lookup.csv

The taxi_zone_lookup.csv has the details for each pickup_location/dropoff_location of the rideshare_data.csv. taxi_zone_lookup.csv has the following schema:

LocationID: string (nullable = true)
Borough: string (nullable = true)
Zone: string (nullable = true)
service_zone: string (nullable = true)

The table below shows the samples from the taxi_zone_lookup.csv:

|LocationID|Borough            |Zone                   |service_zone|
|----------|-------------------|-----------------------|------------|
|1         |EWR                |Newark Airport         |EWR         |
|2         |Queens             |Jamaica Bay            |Boro Zone   |
|3         |Bronx              |Allerton/Pelham Gardens|Boro Zone   |
|4         |Manhattan          |Alphabet City          |Yellow Zone |
|5         |Staten Island      |Arden Heights          |Boro Zone   |

As you can see, the pickup_location/dropoff_location fields in rideshare_data.csv are encoded with numbers that you can find the counterpart number (LocationID field) in taxi_zone_lookup.csv. You need to join the two datasets by using the mentioned fields. Before you go to the assignment part, there are three notes you need to understand about the taxi_zone_lookup.csv, (1): The LocationID 264 and 265 are **Unknown** in the Borough field, we see the 'Unknown' as one of the borough names; (2) In the Borough field, you can see the same Borough has different LocationIDs, it does not matter because you use LocationID (Unique Key) to apply 'Join'function; and (3) if you see any obscure description (like NV, NA, N/A, etc) in any fields, purely see it as the valid names.
