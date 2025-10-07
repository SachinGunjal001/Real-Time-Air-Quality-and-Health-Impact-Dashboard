CREATE DATABASE AirQuality

use AirQuality

CREATE TABLE AirQualityData (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(50),
    aqi INT,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT,
    timestamp DATETIME
);

select * from AirQualityData
