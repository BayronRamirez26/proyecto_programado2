import pandas as pd
import numpy as np
import os

RAW_DIR = 'data/raw'
PROCESSED_DIR = 'data/processed'
os.makedirs(PROCESSED_DIR, exist_ok=True)

def process_transport_data():
    print("Processing Transport Data...")
    input_path = os.path.join(RAW_DIR, 'transport_data.csv')
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    
    # Convert timestamps
    df['scheduled_arrival'] = pd.to_datetime(df['scheduled_arrival'])
    df['actual_arrival'] = pd.to_datetime(df['actual_arrival'])
    
    # Ensure delay_minutes is consistent
    df['calculated_delay'] = (df['actual_arrival'] - df['scheduled_arrival']).dt.total_seconds() / 60
    
    # Categorize Punctuality
    def categorize_punctuality(delay):
        if delay <= 0: return 'Early'
        elif delay <= 5: return 'On Time'
        elif delay <= 15: return 'Late'
        else: return 'Very Late'
        
    df['punctuality_status'] = df['calculated_delay'].apply(categorize_punctuality)
    
    output_path = os.path.join(PROCESSED_DIR, 'transport_processed.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

def process_traffic_data():
    print("Processing Traffic Data...")
    input_path = os.path.join(RAW_DIR, 'traffic_data.csv')
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate Congestion Level
    # Simple logic: High density & low speed = Congested
    def get_congestion_level(row):
        if row['avg_speed'] < 20 and row['vehicle_count'] > 50:
            return 'High Congestion'
        elif row['avg_speed'] < 40 and row['vehicle_count'] > 30:
            return 'Moderate Congestion'
        else:
            return 'Low Congestion'
            
    df['congestion_level'] = df.apply(get_congestion_level, axis=1)
    
    output_path = os.path.join(PROCESSED_DIR, 'traffic_processed.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

def process_energy_data():
    print("Processing Energy Data...")
    input_path = os.path.join(RAW_DIR, 'energy_data.csv')
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Normalize consumption to Energy Efficiency Index (lower is better generally, but depends on fuel)
    # For simplicity, let's just keep the raw metrics but maybe aggregate by vehicle type later in BI
    # We can add a 'cost_estimate' column (dummy values)
    
    fuel_costs = {'Diesel': 1.2, 'Electric': 0.15, 'Hybrid': 0.8, 'CNG': 0.9} # $ per unit
    
    df['estimated_cost'] = df.apply(lambda x: x['total_consumption'] * fuel_costs.get(x['fuel_type'], 1.0), axis=1)
    
    output_path = os.path.join(PROCESSED_DIR, 'energy_processed.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

if __name__ == "__main__":
    process_transport_data()
    process_traffic_data()
    process_energy_data()
    print("Preprocessing complete.")
