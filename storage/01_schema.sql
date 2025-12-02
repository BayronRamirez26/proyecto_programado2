CREATE SCHEMA IF NOT EXISTS analytics;

SET search_path = analytics, public;

CREATE TABLE IF NOT EXISTS transport (
  route_id TEXT NOT NULL,
  trip_id TEXT PRIMARY KEY,
  stop_id INTEGER,
  scheduled_arrival TIMESTAMP WITH TIME ZONE,
  actual_arrival TIMESTAMP WITH TIME ZONE,
  delay_minutes INTEGER,
  calculated_delay DOUBLE PRECISION,
  punctuality_status TEXT
);

CREATE TABLE IF NOT EXISTS traffic (
  sensor_id TEXT NOT NULL,
  zone_id TEXT,
  ts TIMESTAMP WITH TIME ZONE,
  vehicle_count INTEGER,
  avg_speed INTEGER,
  congestion_level TEXT
);

CREATE TABLE IF NOT EXISTS energy (
  vehicle_id TEXT NOT NULL,
  fuel_type TEXT,
  ts TIMESTAMP WITH TIME ZONE,
  distance_km DOUBLE PRECISION,
  consumption_rate DOUBLE PRECISION,
  total_consumption DOUBLE PRECISION,
  unit TEXT,
  estimated_cost DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_transport_route ON transport(route_id);
CREATE INDEX IF NOT EXISTS idx_transport_sched ON transport(scheduled_arrival);
CREATE INDEX IF NOT EXISTS idx_traffic_zone_ts ON traffic(zone_id, ts);
CREATE INDEX IF NOT EXISTS idx_energy_vehicle_ts ON energy(vehicle_id, ts);
