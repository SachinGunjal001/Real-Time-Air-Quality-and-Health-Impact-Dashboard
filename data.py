import requests
import pandas as pd
import pyodbc
from datetime import datetime

# ----------------------------
# ----------------------------
api_key = "c623d8f4c6c0bc25a43e29e666859fb3"
server = r"SACHIN\SQLEXPRESS"
database = "AirQuality"
table_name = "WeatherData"

# ----------------------------
# ----------------------------
locations = []

def add_location(city, lat, lon):
    """
    Add a new city to the locations list.
    """
    locations.append({"city": city, "lat": lat, "lon": lon})
    print(f" Added location: {city} (Lat: {lat}, Lon: {lon})")

# Add cities here (you can expand later)
add_location("New York", 40.7128, -74.0060)
add_location("Los Angeles", 34.0522, -118.2437)
add_location("Chicago", 41.8781, -87.6298)
add_location("Houston", 29.7604, -95.3698)
add_location("Miami", 25.7617, -80.1918)
add_location("Toronto", 43.651070, -79.347015)
add_location("Vancouver", 49.2827, -123.1207)
add_location("Mexico City", 19.4326, -99.1332)
add_location("São Paulo", -23.5505, -46.6333)
add_location("Buenos Aires", -34.6037, -58.3816)
add_location("London", 51.5074, -0.1278)
add_location("Paris", 48.8566, 2.3522)
add_location("Berlin", 52.5200, 13.4050)
add_location("Madrid", 40.4168, -3.7038)
add_location("Rome", 41.9028, 12.4964)
add_location("Lisbon", 38.7169, -9.1390)
add_location("Amsterdam", 52.3676, 4.9041)
add_location("Brussels", 50.8503, 4.3517)
add_location("Zurich", 47.3769, 8.5417)
add_location("Vienna", 48.2082, 16.3738)
add_location("Prague", 50.0755, 14.4378)
add_location("Warsaw", 52.2297, 21.0122)
add_location("Moscow", 55.7558, 37.6173)
add_location("Istanbul", 41.0082, 28.9784)
add_location("Dubai", 25.276987, 55.296249)
add_location("Riyadh", 24.7136, 46.6753)
add_location("Doha", 25.2854, 51.5310)
add_location("Tehran", 35.6892, 51.3890)
add_location("Cairo", 30.0444, 31.2357)
add_location("Nairobi", -1.2921, 36.8219)
add_location("Johannesburg", -26.2041, 28.0473)
add_location("Cape Town", -33.9249, 18.4241)
add_location("Lagos", 6.5244, 3.3792)
add_location("Casablanca", 33.5731, -7.5898)
add_location("Mumbai", 19.0760, 72.8777)
add_location("Delhi", 28.6139, 77.2090)
add_location("Bangalore", 12.9716, 77.5946)
add_location("Chennai", 13.0827, 80.2707)
add_location("Hyderabad", 17.3850, 78.4867)
add_location("Kolkata", 22.5726, 88.3639)
add_location("Ahmedabad", 23.0225, 72.5714)
add_location("Pune", 18.5246, 73.8528)
add_location("Jaipur", 26.9124, 75.7873)
add_location("Lucknow", 26.8467, 80.9462)
add_location("Patna", 25.5941, 85.1376)
add_location("Indore", 22.7196, 75.8577)
add_location("Nagpur", 21.1458, 79.0882)
add_location("Surat", 21.1702, 72.8311)
add_location("Bhopal", 23.2599, 77.4126)
add_location("Varanasi", 25.3176, 82.9739)
add_location("Singapore", 1.3521, 103.8198)
add_location("Bangkok", 13.7563, 100.5018)
add_location("Kuala Lumpur", 3.1390, 101.6869)
add_location("Jakarta", -6.2088, 106.8456)
add_location("Manila", 14.5995, 120.9842)
add_location("Ho Chi Minh City", 10.8231, 106.6297)
add_location("Hanoi", 21.0278, 105.8342)
add_location("Hong Kong", 22.3193, 114.1694)
add_location("Beijing", 39.9042, 116.4074)
add_location("Shanghai", 31.2304, 121.4737)
add_location("Seoul", 37.5665, 126.9780)
add_location("Tokyo", 35.6895, 139.6917)
add_location("Osaka", 34.6937, 135.5023)
add_location("Taipei", 25.0330, 121.5654)
add_location("Sydney", -33.8688, 151.2093)
add_location("Melbourne", -37.8136, 144.9631)
add_location("Brisbane", -27.4698, 153.0251)
add_location("Perth", -31.9505, 115.8605)
add_location("Auckland", -36.8485, 174.7633)
add_location("Wellington", -41.2865, 174.7762)
add_location("Christchurch", -43.5321, 172.6362)
add_location("Honolulu", 21.3069, -157.8583)
add_location("San Francisco", 37.7749, -122.4194)
add_location("Seattle", 47.6062, -122.3321)
add_location("Boston", 42.3601, -71.0589)
add_location("Dallas", 32.7767, -96.7970)
add_location("Atlanta", 33.7490, -84.3880)
add_location("Washington DC", 38.9072, -77.0369)
add_location("Philadelphia", 39.9526, -75.1652)
add_location("Denver", 39.7392, -104.9903)
add_location("Las Vegas", 36.1699, -115.1398)
add_location("Phoenix", 33.4484, -112.0740)
add_location("San Diego", 32.7157, -117.1611)
add_location("Montreal", 45.5017, -73.5673)
add_location("Bogotá", 4.7110, -74.0721)
add_location("Lima", -12.0464, -77.0428)
add_location("Santiago", -33.4489, -70.6693)
add_location("Caracas", 10.4806, -66.9036)
add_location("Rio de Janeiro", -22.9068, -43.1729)
add_location("San Juan", 18.4655, -66.1057)
add_location("Reykjavik", 64.1466, -21.9426)
add_location("Oslo", 59.9139, 10.7522)
add_location("Stockholm", 59.3293, 18.0686)
add_location("Copenhagen", 55.6761, 12.5683)
add_location("Helsinki", 60.1699, 24.9384)
add_location("Dublin", 53.3498, -6.2603)
add_location("Edinburgh", 55.9533, -3.1883)
add_location("Glasgow", 55.8642, -4.2518)
add_location("Manama", 26.2235, 50.5876)
add_location("Muscat", 23.5880, 58.3829)
add_location("Tel Aviv", 32.0853, 34.7818)
add_location("Jerusalem", 31.7683, 35.2137)

# ----------------------------
# ----------------------------
def get_weather_data(lat, lon, city):
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
        response = requests.get(url, params=params)
        data = response.json()

        if response.status_code == 200 and "main" in data:
            return {
                "City": city,
                "Temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "Humidity": data["main"]["humidity"],
                "Pressure": data["main"]["pressure"],
                "recorded_at": datetime.now()  #  Matches SQL column
            }
        else:
            print(f"API error for {city}: {data}")
            return None
    except Exception as e:
        print(f" Error fetching data for {city}: {e}")
        return None

# ----------------------------
# ----------------------------
def save_to_mssql(df):
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            City VARCHAR(50) NOT NULL,
            Temperature DECIMAL(5,2) NOT NULL,
            feels_like DECIMAL(5,2) NOT NULL,
            Humidity INT CHECK (Humidity BETWEEN 0 AND 100) NOT NULL,
            Pressure INT CHECK (Pressure > 0) NOT NULL,
            recorded_at DATETIME DEFAULT GETDATE()
        );
        """)

        # Insert each record into SQL
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} (City, Temperature, feels_like, Humidity, Pressure, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
            row["City"], row["Temperature"], row["feels_like"],
            row["Humidity"], row["Pressure"], row["recorded_at"])

        conn.commit()
        print(f" Successfully inserted {len(df)} rows into {table_name}")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f" Error saving to MSSQL: {e}")

# ----------------------------
# ----------------------------
def run_etl():
    weather_data = []
    for loc in locations:
        print(f"Fetching weather data for {loc['city']}...")
        record = get_weather_data(loc["lat"], loc["lon"], loc["city"])
        if record:
            weather_data.append(record)

    if weather_data:
        df = pd.DataFrame(weather_data)
        save_to_mssql(df)
    else:
        print(" No weather data fetched — check API or internet connection.")

# ----------------------------
#  RUN SCRIPT
# ----------------------------
if __name__ == "__main__":
    run_etl()
