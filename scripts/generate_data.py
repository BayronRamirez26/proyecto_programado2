import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)

DATA_DIR = 'data/raw'
os.makedirs(DATA_DIR, exist_ok=True)

def generate_transport_data(n_rows=1000):
    print("Generating Transport Data...")
    data = []
    routes = ['Route_A', 'Route_B', 'Route_C', 'Route_D', 'Route_E']
    
    for _ in range(n_rows):
        route_id = random.choice(routes)
        trip_id = fake.uuid4()
        stop_id = random.randint(1, 20)
        
        # Generate scheduled time
        scheduled_arrival = fake.date_time_between(start_date='-1M', end_date='now')
        
        # Generate actual time with some delay logic
        # 70% chance of being on time or early, 30% chance of delay
        if random.random() < 0.7:
            delay = random.randint(-2, 5) # -2 mins early to 5 mins late (acceptable)
        else:
            delay = random.randint(5, 45) # Significant delay
            
        actual_arrival = scheduled_arrival + timedelta(minutes=delay)
        
        data.append({
            'route_id': route_id,
            'trip_id': trip_id,
            'stop_id': stop_id,
            'scheduled_arrival': scheduled_arrival,
            'actual_arrival': actual_arrival,
            'delay_minutes': delay 
        })
    
    df = pd.DataFrame(data)
    output_path = os.path.join(DATA_DIR, 'transport_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

def generate_traffic_data(n_rows=1000):
    print("Generating Traffic Data...")
    data = []
    zones = ['Zone_North', 'Zone_South', 'Zone_East', 'Zone_West', 'Zone_Center']
    
    for _ in range(n_rows):
        sensor_id = f"SENS_{random.randint(100, 999)}"
        zone_id = random.choice(zones)
        timestamp = fake.date_time_between(start_date='-1M', end_date='now')
        
        # Correlate speed and vehicle count roughly
        vehicle_count = random.randint(0, 100)
        if vehicle_count > 80:
            avg_speed = random.randint(5, 20) # Congestion
        elif vehicle_count > 40:
            avg_speed = random.randint(20, 45) # Moderate
        else:
            avg_speed = random.randint(45, 80) # Free flow
            
        data.append({
            'sensor_id': sensor_id,
            'zone_id': zone_id,
            'timestamp': timestamp,
            'vehicle_count': vehicle_count,
            'avg_speed': avg_speed
        })
        
    df = pd.DataFrame(data)
    output_path = os.path.join(DATA_DIR, 'traffic_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

def generate_energy_data(n_rows=500):
    print("Generating Energy Data...")
    data = []
    fuel_types = ['Diesel', 'Electric', 'Hybrid', 'CNG']
    
    for _ in range(n_rows):
        vehicle_id = f"BUS_{random.randint(1000, 9999)}"
        fuel_type = random.choice(fuel_types)
        timestamp = fake.date_time_between(start_date='-1M', end_date='now')
        distance_km = random.uniform(5.0, 50.0)
        
        # Consumption based on fuel type
        if fuel_type == 'Electric':
            consumption_rate = random.uniform(0.8, 1.5) # kWh/km
            unit = 'kWh'
        elif fuel_type == 'Diesel':
            consumption_rate = random.uniform(0.3, 0.6) # L/km
            unit = 'L'
        else:
            consumption_rate = random.uniform(0.2, 0.5) # Mix
            unit = 'L_eq'
            
        total_consumption = distance_km * consumption_rate
        
        data.append({
            'vehicle_id': vehicle_id,
            'fuel_type': fuel_type,
            'timestamp': timestamp,
            'distance_km': round(distance_km, 2),
            'consumption_rate': round(consumption_rate, 2),
            'total_consumption': round(total_consumption, 2),
            'unit': unit
        })
        
    df = pd.DataFrame(data)
    output_path = os.path.join(DATA_DIR, 'energy_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

if __name__ == "__main__":
    generate_transport_data(2000)
    generate_traffic_data(2000)
    generate_energy_data(1000)
    print("Data generation complete.")
