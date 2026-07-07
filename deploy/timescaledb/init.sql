-- TimescaleDB hypertable migration for MarketPulse time-series tables.
-- Safe to run on plain PostgreSQL (extension commands are no-ops when unavailable).

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('stock_ticks', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);
        PERFORM create_hypertable('stock_features', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);
        PERFORM create_hypertable('stock_anomalies', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);
    END IF;
EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'Tables not yet created — hypertables will be applied after schema init';
    WHEN OTHERS THEN
        RAISE NOTICE 'TimescaleDB hypertable setup skipped: %', SQLERRM;
END $$;
