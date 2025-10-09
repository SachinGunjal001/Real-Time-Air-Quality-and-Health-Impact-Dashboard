CREATE TABLE WeatherData (
    id INT IDENTITY(1,1) PRIMARY KEY, 
	City VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,         
    feels_like DECIMAL(5,2) NOT NULL,          
    humidity INT CHECK (humidity BETWEEN 0 AND 100) NOT NULL, 
    pressure INT CHECK (pressure > 0) NOT NULL, 
    recorded_at DATETIME DEFAULT GETDATE()    
);

select * from WeatherData