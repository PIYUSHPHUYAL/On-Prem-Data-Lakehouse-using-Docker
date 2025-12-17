-- Initialize PostgreSQL Database for Lakehouse Serving Layer

-- Create schema for gold layer tables
CREATE SCHEMA IF NOT EXISTS gold;

-- Create a metadata table to track pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    layer VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline 
ON pipeline_metadata(pipeline_name, started_at DESC);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON SCHEMA gold TO lakehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO lakehouse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO lakehouse;
GRANT ALL PRIVILEGES ON TABLE pipeline_metadata TO lakehouse;

-- Log successful initialization
INSERT INTO pipeline_metadata (pipeline_name, layer, status, records_processed)
VALUES ('database_init', 'system', 'completed', 0);
