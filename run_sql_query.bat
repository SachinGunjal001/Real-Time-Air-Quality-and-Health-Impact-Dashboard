@echo off
sqlcmd -S SACHIN\SQLEXPRESS -d AirQuality -E -Q "select * from dbo.WeatherData;"
pause
