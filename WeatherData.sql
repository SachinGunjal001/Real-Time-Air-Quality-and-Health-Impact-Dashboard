use AirQuality

CREATE TABLE WeatherData (
	City VARCHAR(50) NOT NULL,
    Temperature DECIMAL(5,2) NOT NULL,         
    feels_like DECIMAL(5,2) NOT NULL,          
    Humidity INT CHECK (Humidity BETWEEN 0 AND 100) NOT NULL, 
    Pressure INT CHECK (Pressure > 0) NOT NULL, 
    recorded_at DATETIME DEFAULT GETDATE()    
);

select top 150 * from WeatherData
order by recorded_at desc;


select * from WeatherData

select count(*) from WeatherData