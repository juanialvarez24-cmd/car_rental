-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS aereo;

-- Use the database
USE aereo;

-- Create the external table
CREATE EXTERNAL TABLE IF NOT EXISTS car_rental_analytics (
    fuelType STRING,
    rating INT,
    renterTripsTaken INT,
    reviewCount INT,
    city STRING,
    state_name STRING,
    owner_id INT,
    rate_daily INT,
    make STRING,
    model STRING,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/hive/external/car_rental/analytics'
TBLPROPERTIES ('skip.header.line.count' = '1');
