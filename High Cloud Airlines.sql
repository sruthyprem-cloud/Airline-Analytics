create database Airlines;

use Airlines;



SET GLOBAL local_infile = ON;

CREATE TABLE main_data (
    airline_id INT,
    carrier_group_id INT,
    unique_carrier_code VARCHAR(10),
    unique_carrier_entity_code INT,
    region_code CHAR(1),

    origin_airport_id INT,
    origin_airport_sequence_id INT,
    origin_airport_market_id INT,
    origin_world_area_code INT,

    destination_airport_id INT,
    destination_airport_sequence_id INT,
    destination_airport_market_id INT,
    destination_world_area_code INT,

    aircraft_group_id INT,
    aircraft_type_id INT,
    aircraft_configuration_id INT,
    distance_group_id INT,
    service_class_id CHAR(1),
    datasource_id CHAR(2),

    departures_scheduled INT,
    departures_performed INT,
    payload INT,
    distance INT,
    available_seats INT,
    transported_passengers INT,
    transported_freight INT,
    transported_mail INT,
    ramp_to_ramp_time INT,
    air_time INT,

    unique_carrier VARCHAR(100),
    carrier_code VARCHAR(10),
    carrier_name VARCHAR(150),

    origin_airport_code VARCHAR(10),
    origin_city VARCHAR(100),
    origin_state_code CHAR(2),
    origin_state_fips INT,
    origin_state VARCHAR(50),
    origin_country_code CHAR(2),
    origin_country VARCHAR(50),

    destination_airport_code VARCHAR(10),
    destination_city VARCHAR(100),
    destination_state_code CHAR(2),
    destination_state_fips INT,
    destination_state VARCHAR(50),
    destination_country_code CHAR(2),
    destination_country VARCHAR(50),

    year INT,
    month INT,
    day INT,

    from_to_airport_code VARCHAR(20),
    from_to_airport_id VARCHAR(30),
    from_to_city VARCHAR(150),
    from_to_state_code VARCHAR(20),
    from_to_state VARCHAR(100)
);

ALTER TABLE main_data
ADD COLUMN flight_date DATE;

UPDATE main_data
SET flight_date = STR_TO_DATE(
    CONCAT(year,'-',month,'-',day),
    '%Y-%m-%d'
);

LOAD DATA LOCAL INFILE 'D:\\Madan files\\ExcelR\\Excel R Projects\\Airlines\\MainData_Final.csv'
INTO TABLE main_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



CREATE TABLE distance_groups (
    distance_group_id INT PRIMARY KEY,
    distance_interval VARCHAR(100)
);

LOAD DATA LOCAL INFILE 'D:\\Madan files\\ExcelR\\Excel R Projects\\Airlines\\Files\\Distance Groups.csv'
INTO TABLE distance_groups
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;










#Queries

# Single KPI's 
# a)Total Flights
SELECT 
    CONCAT(ROUND(SUM(departures_performed) / 1000000, 2), ' M') AS total_flights_m
FROM main_data;



# b)Total Passengers
SELECT 
    CONCAT(ROUND(SUM(transported_passengers) / 1000000, 2), ' M') AS total_passengers
FROM main_data;



# c)Overall Load Factor
SELECT
    concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),'%') AS overall_load_factor_pct
FROM main_data;



# 1)Load Factor % – Yearly / Quarterly / Monthly
SELECT
    year,
    Concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),'%') AS load_factor_pct
FROM main_data
GROUP BY year
ORDER BY year;

SELECT
    QUARTER(flight_date) AS quarter,
    Concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),'%') AS load_factor_pct
FROM main_data
GROUP BY quarter
ORDER BY quarter;

SELECT
    month,
    Concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),'%') AS load_factor_pct
FROM main_data
GROUP BY month
ORDER BY month;



# 2)Load Factor % by Carrier Name

SELECT
    carrier_name,
    concat(ROUND(
        SUM(transported_passengers) / NULLIF(SUM(available_seats), 0) * 100,
        2
    ),'%') AS load_factor_pct
FROM main_data
GROUP BY carrier_name
HAVING load_factor_pct IS NOT NULL
ORDER BY load_factor_pct DESC;



CREATE TABLE KPI2 AS
SELECT
    carrier_name,
    ROUND(
        SUM(transported_passengers) / NULLIF(SUM(available_seats), 0) * 100,
        2
    ) AS load_factor_pct
FROM main_data
GROUP BY carrier_name
HAVING load_factor_pct IS NOT NULL
ORDER BY load_factor_pct DESC;



# 3)Top 10 Carrier Names by Passenger Preference

SELECT
    carrier_name,
   concat( ROUND(count(transported_passengers) / 1000, 2),' K') AS passenger_preference_k
FROM main_data
GROUP BY carrier_name
ORDER BY passenger_preference_k DESC
LIMIT 10;

create table KPI3 as SELECT
    carrier_name,
   concat( ROUND(count(transported_passengers) / 1000, 2),' K') AS passenger_preference_k
FROM main_data
GROUP BY carrier_name
ORDER BY passenger_preference_k DESC
LIMIT 10;




# 4)Top Routes (From–To City) by Number of Flights
SELECT
    CONCAT(origin_city, ' → ', destination_city) AS route,
   concat(round( SUM(departures_performed)/ 1000,2),' K') AS number_of_flights
FROM main_data
GROUP BY route
ORDER BY number_of_flights DESC
LIMIT 10;

create table kpi4 as SELECT
    CONCAT(origin_city, ' → ', destination_city) AS route,
   concat(round( SUM(departures_performed)/ 1000,2),' K') AS number_of_flights
FROM main_data
GROUP BY route
ORDER BY number_of_flights DESC
LIMIT 10;


# 5)Load Factor – Weekend vs Weekday
SELECT
    CASE
        WHEN DAYOFWEEK(flight_date) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    Concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),' %') AS load_factor_pct
FROM main_data
GROUP BY day_type;

create table kpi5 as SELECT
    CASE
        WHEN DAYOFWEEK(flight_date) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    Concat(ROUND(SUM(transported_passengers) / SUM(available_seats) * 100, 2),' %') AS load_factor_pct
FROM main_data
GROUP BY day_type;



# 6)Number of Flights by Distance Group
SELECT
    d.distance_interval,
    count(m.departures_performed) AS number_of_flights
FROM main_data as m
join distance_groups as d on m.distance_group_id = d.distance_group_id
GROUP BY d.distance_group_id
ORDER BY number_of_flights DESC;


create table kpi6 as SELECT
    d.distance_interval,
    count(m.departures_performed) AS number_of_flights
FROM main_data as m
join distance_groups as d on m.distance_group_id = d.distance_group_id
GROUP BY d.distance_group_id
ORDER BY number_of_flights DESC;
