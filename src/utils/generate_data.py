import csv
import random
from datetime import datetime, timedelta
import os

def generate_rides(num_rows=1000, output_file='../../data/rides.csv'):
    print(f"Generating {num_rows} rides to {output_file}...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    stations = [
        {"id": "S1", "name": "Central Park West & W 72 St", "lat": 40.778, "lon": -73.974},
        {"id": "S2", "name": "5 Ave & E 73 St", "lat": 40.771, "lon": -73.966},
        {"id": "S3", "name": "W 42 St & 8 Ave", "lat": 40.757, "lon": -73.990},
        {"id": "S4", "name": "E 42 St & 2 Ave", "lat": 40.750, "lon": -73.973},
        {"id": "S5", "name": "Wall St", "lat": 40.707, "lon": -74.010},
        {"id": "S6", "name": "South St Seaport", "lat": 40.705, "lon": -74.003},
    ]
    
    rideable_types = ["electric_bike", "classic_bike"]
    member_types = ["member", "casual"]
    
    start_time = datetime(2024, 6, 1, 8, 0, 0)
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "ride_id", "rideable_type", "started_at", "ended_at",
            "start_station_name", "start_station_id", "end_station_name", "end_station_id",
            "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"
        ])
        
        for i in range(num_rows):
            start_station = random.choice(stations)
            end_station = random.choice(stations)
            while end_station == start_station:
                end_station = random.choice(stations)
                
            duration_mins = random.randint(5, 45)
            ride_start = start_time + timedelta(minutes=random.randint(0, 5)) # Rides start every few mins
            ride_end = ride_start + timedelta(minutes=duration_mins)
            start_time = ride_start # Increment base time
            
            writer.writerow([
                f"RIDE_{i:06d}",
                random.choice(rideable_types),
                ride_start.strftime("%Y-%m-%d %H:%M:%S"),
                ride_end.strftime("%Y-%m-%d %H:%M:%S"),
                start_station["name"], start_station["id"],
                end_station["name"], end_station["id"],
                start_station["lat"], start_station["lon"],
                end_station["lat"], end_station["lon"],
                random.choice(member_types)
            ])
            
    print("Done!")

if __name__ == "__main__":
    generate_rides()
