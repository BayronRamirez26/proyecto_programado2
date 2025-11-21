# Data Dictionary

## 1. Transport Data
**File:** `data/processed/transport_processed.csv`
**Description:** Contains schedule and actual arrival times for public transport routes.

| Column | Type | Description |
|--------|------|-------------|
| `route_id` | String | Identifier of the bus route (e.g., Route_A) |
| `trip_id` | String | Unique identifier for the specific trip |
| `stop_id` | Integer | Identifier of the stop |
| `scheduled_arrival` | DateTime | Planned arrival time |
| `actual_arrival` | DateTime | Actual arrival time |
| `delay_minutes` | Integer | Difference in minutes (Actual - Scheduled). Positive = Late. |
| `calculated_delay` | Float | Precise delay calculated during ETL. |
| `punctuality_status` | String | Category: Early, On Time, Late, Very Late. |

## 2. Traffic Data
**File:** `data/processed/traffic_processed.csv`
**Description:** Sensor data measuring vehicle flow and speed in different zones.

| Column | Type | Description |
|--------|------|-------------|
| `sensor_id` | String | Unique ID of the traffic sensor |
| `zone_id` | String | Geographic zone (e.g., Zone_North) |
| `timestamp` | DateTime | Time of measurement |
| `vehicle_count` | Integer | Number of vehicles detected |
| `avg_speed` | Integer | Average speed in km/h |
| `congestion_level` | String | Derived category: Low, Moderate, High Congestion. |

## 3. Energy Data
**File:** `data/processed/energy_processed.csv`
**Description:** Telemetry data from the transport fleet regarding fuel consumption.

| Column | Type | Description |
|--------|------|-------------|
| `vehicle_id` | String | Unique ID of the vehicle |
| `fuel_type` | String | Diesel, Electric, Hybrid, CNG |
| `timestamp` | DateTime | Time of recording |
| `distance_km` | Float | Distance traveled in this segment |
| `consumption_rate` | Float | Rate of consumption (L/km or kWh/km) |
| `total_consumption` | Float | Total fuel/energy used |
| `unit` | String | Unit of measurement (L, kWh) |
| `estimated_cost` | Float | Estimated cost of the trip segment. |
