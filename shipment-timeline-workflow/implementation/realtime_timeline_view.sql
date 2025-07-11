-- Real-time Shipment Timeline View
-- Combines base daily processing with incremental updates
-- This is the query end-users should use for current timeline data

-- Create materialized view for performance (refresh as needed)
CREATE MATERIALIZED VIEW IF NOT EXISTS shipment_timeline_realtime AS

WITH combined_timeline AS (
    -- Base timeline data (bulk historical data)
    SELECT 
        shipment_id,
        event_timestamp,
        event_type,
        location_id,
        destination_location_id,
        package_id,
        status,
        scan_type,
        carrier_code,
        facility_type,
        location_name,
        region,
        country_code,
        package_weight,
        package_dimensions,
        service_level,
        event_sequence,
        prev_event_timestamp,
        prev_location_id,
        prev_status,
        next_event_timestamp,
        next_location_id,
        time_since_prev_event,
        time_to_next_event,
        events_at_location,
        cumulative_transit_hours,
        first_event_timestamp,
        delivery_timestamp,
        total_shipment_hours,
        progress_percentage,
        timeline_key,
        processed_timestamp,
        'base' as data_source,
        created_date,
        updated_date
    FROM shipment_timeline_base
    
    UNION ALL
    
    -- Incremental updates (recent data)
    SELECT 
        shipment_id,
        event_timestamp,
        event_type,
        location_id,
        destination_location_id,
        package_id,
        status,
        scan_type,
        carrier_code,
        facility_type,
        location_name,
        region,
        country_code,
        package_weight,
        package_dimensions,
        service_level,
        event_sequence,
        prev_event_timestamp,
        prev_location_id,
        prev_status,
        next_event_timestamp,
        next_location_id,
        time_since_prev_event,
        time_to_next_event,
        events_at_location,
        cumulative_transit_hours,
        first_event_timestamp,
        delivery_timestamp,
        total_shipment_hours,
        progress_percentage,
        timeline_key,
        processed_timestamp,
        'incremental' as data_source,
        created_date,
        updated_date
    FROM shipment_timeline_incremental
),

-- Fix any sequencing issues across base/incremental boundary
corrected_timeline AS (
    SELECT 
        *,
        -- Recalculate event sequence across the full timeline
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as corrected_event_sequence,
        
        -- Recalculate next event timestamp across boundary
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as corrected_next_event_timestamp,
        
        -- Recalculate time to next event
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) - event_timestamp as corrected_time_to_next_event,
        
        -- Fix events at location count (aggregate across both sources)
        COUNT(*) OVER (
            PARTITION BY shipment_id, location_id
        ) as corrected_events_at_location
        
    FROM combined_timeline
)

-- Final timeline with all corrections applied
SELECT 
    shipment_id,
    event_timestamp,
    event_type,
    location_id,
    destination_location_id,
    package_id,
    status,
    scan_type,
    carrier_code,
    facility_type,
    location_name,
    region,
    country_code,
    package_weight,
    package_dimensions,
    service_level,
    
    -- Use corrected sequence and timing
    corrected_event_sequence as event_sequence,
    prev_event_timestamp,
    prev_location_id,
    prev_status,
    corrected_next_event_timestamp as next_event_timestamp,
    next_location_id,
    time_since_prev_event,
    corrected_time_to_next_event as time_to_next_event,
    corrected_events_at_location as events_at_location,
    
    -- Keep original cumulative calculations (they should be correct)
    cumulative_transit_hours,
    first_event_timestamp,
    delivery_timestamp,
    
    -- Recalculate total shipment time if delivered
    CASE 
        WHEN delivery_timestamp IS NOT NULL THEN
            EXTRACT(EPOCH FROM (delivery_timestamp - first_event_timestamp)) / 3600.0
        ELSE NULL
    END as total_shipment_hours,
    
    progress_percentage,
    
    -- Updated timeline key
    shipment_id || '|' || corrected_event_sequence::text as timeline_key,
    
    processed_timestamp,
    data_source,
    created_date,
    updated_date,
    
    -- Add useful derived fields for analysis
    EXTRACT(DOW FROM event_timestamp) as event_day_of_week,
    EXTRACT(HOUR FROM event_timestamp) as event_hour,
    DATE_TRUNC('hour', event_timestamp) as event_hour_bucket,
    
    -- Current status indicators
    CASE 
        WHEN status = 'delivered' THEN TRUE
        ELSE FALSE
    END as is_delivered,
    
    CASE 
        WHEN event_timestamp = MAX(event_timestamp) OVER (PARTITION BY shipment_id) 
        THEN TRUE ELSE FALSE
    END as is_latest_event,
    
    -- Performance indicators
    CASE 
        WHEN time_since_prev_event > INTERVAL '24 hours' THEN 'delayed'
        WHEN time_since_prev_event > INTERVAL '12 hours' THEN 'slow'
        ELSE 'normal'
    END as event_timing_status,
    
    -- Location transition indicators
    CASE 
        WHEN location_id != prev_location_id THEN TRUE
        ELSE FALSE
    END as is_location_change,
    
    -- Add current timestamp for data freshness tracking
    CURRENT_TIMESTAMP as view_generated_at

FROM corrected_timeline
ORDER BY shipment_id, event_timestamp;

-- Create indexes on the materialized view for performance
CREATE INDEX IF NOT EXISTS idx_realtime_timeline_shipment_id 
ON shipment_timeline_realtime(shipment_id);

CREATE INDEX IF NOT EXISTS idx_realtime_timeline_timestamp 
ON shipment_timeline_realtime(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_realtime_timeline_status 
ON shipment_timeline_realtime(status);

CREATE INDEX IF NOT EXISTS idx_realtime_timeline_location 
ON shipment_timeline_realtime(location_id);

CREATE INDEX IF NOT EXISTS idx_realtime_timeline_latest 
ON shipment_timeline_realtime(shipment_id, is_latest_event) 
WHERE is_latest_event = TRUE;

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_realtime_timeline_composite 
ON shipment_timeline_realtime(shipment_id, event_timestamp, status, location_id);

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_shipment_timeline_realtime()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY shipment_timeline_realtime;
    
    -- Log the refresh
    INSERT INTO processing_stats (
        checkpoint_id,
        process_date,
        records_processed,
        processing_duration_seconds,
        process_type
    )
    SELECT 
        'realtime_view_refresh',
        CURRENT_DATE,
        COUNT(*),
        0,  -- Duration calculated separately if needed
        'view_refresh'
    FROM shipment_timeline_realtime;
    
END;
$$ LANGUAGE plpgsql;

-- Create view for easy querying without needing to know about materialized view
CREATE OR REPLACE VIEW shipment_timeline_current AS
SELECT * FROM shipment_timeline_realtime;

-- Helper views for common use cases

-- Latest status per shipment
CREATE OR REPLACE VIEW shipment_latest_status AS
SELECT DISTINCT
    shipment_id,
    event_timestamp as last_event_timestamp,
    status as current_status,
    location_id as current_location_id,
    location_name as current_location_name,
    progress_percentage,
    is_delivered,
    total_shipment_hours,
    cumulative_transit_hours,
    first_event_timestamp,
    delivery_timestamp,
    data_source as latest_update_source
FROM shipment_timeline_realtime
WHERE is_latest_event = TRUE;

-- Active shipments (not yet delivered)
CREATE OR REPLACE VIEW active_shipments AS
SELECT *
FROM shipment_timeline_current
WHERE NOT is_delivered
ORDER BY shipment_id, event_timestamp;

-- Delivered shipments with summary metrics
CREATE OR REPLACE VIEW delivered_shipments_summary AS
SELECT 
    shipment_id,
    first_event_timestamp,
    delivery_timestamp,
    total_shipment_hours,
    cumulative_transit_hours,
    COUNT(*) as total_events,
    COUNT(DISTINCT location_id) as locations_visited,
    MAX(CASE WHEN event_timing_status = 'delayed' THEN 1 ELSE 0 END) as had_delays
FROM shipment_timeline_current
WHERE is_delivered = TRUE
GROUP BY 
    shipment_id, 
    first_event_timestamp, 
    delivery_timestamp, 
    total_shipment_hours, 
    cumulative_transit_hours
ORDER BY delivery_timestamp DESC;

-- Performance monitoring view
CREATE OR REPLACE VIEW timeline_processing_health AS
WITH processing_lag AS (
    SELECT 
        MAX(event_timestamp) as latest_event_in_data,
        MAX(processed_timestamp) as latest_processing_time,
        CURRENT_TIMESTAMP - MAX(event_timestamp) as data_lag,
        CURRENT_TIMESTAMP - MAX(processed_timestamp) as processing_lag
    FROM shipment_timeline_current
),
data_distribution AS (
    SELECT 
        data_source,
        COUNT(*) as record_count,
        MIN(event_timestamp) as oldest_event,
        MAX(event_timestamp) as newest_event
    FROM shipment_timeline_current
    GROUP BY data_source
)
SELECT 
    pl.*,
    json_agg(dd.*) as data_source_distribution
FROM processing_lag pl, data_distribution dd
GROUP BY 
    pl.latest_event_in_data,
    pl.latest_processing_time,
    pl.data_lag,
    pl.processing_lag;