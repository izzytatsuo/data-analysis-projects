-- Daily Base Process for Shipment Timeline
-- Runs at 00:00 UTC daily to create baseline timeline for last 10 days
-- This is the "expensive" operation that computes all window functions

-- Step 1: Create checkpoint table if not exists
CREATE TABLE IF NOT EXISTS processing_checkpoints (
    checkpoint_id VARCHAR(50) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    process_type VARCHAR(20),
    status VARCHAR(20) DEFAULT 'active'
);

-- Step 2: Initialize or get current checkpoint
INSERT INTO processing_checkpoints (checkpoint_id, last_processed_timestamp, process_type)
VALUES ('shipment_timeline', CURRENT_DATE - INTERVAL '1 day', 'daily_base')
ON CONFLICT (checkpoint_id) DO NOTHING;

-- Step 3: Drop and recreate base timeline table for clean slate
DROP TABLE IF EXISTS shipment_timeline_base;

CREATE TABLE shipment_timeline_base AS
WITH base_data AS (
    -- Extract 10 days of shipment events
    -- Adjust source table name based on your actual schema
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
        -- Add other relevant fields from your shipment_timeline queries
        created_date,
        updated_date
    FROM shipment_events  -- Adjust table name
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL '10 days'
        AND event_timestamp < CURRENT_DATE
        AND status IS NOT NULL  -- Filter out invalid records
),

-- Step 4: Enrich with location and package details
enriched_data AS (
    SELECT 
        bd.*,
        -- Add location enrichment if you have location tables
        loc.location_name,
        loc.region,
        loc.country_code,
        -- Add package details if available
        pkg.package_weight,
        pkg.package_dimensions,
        pkg.service_level
    FROM base_data bd
    LEFT JOIN locations loc ON bd.location_id = loc.location_id
    LEFT JOIN packages pkg ON bd.package_id = pkg.package_id
),

-- Step 5: Compute window functions (the expensive part)
timeline_with_windows AS (
    SELECT 
        *,
        
        -- Event sequencing within shipment
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as event_sequence,
        
        -- Previous event details
        LAG(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as prev_event_timestamp,
        
        LAG(location_id) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as prev_location_id,
        
        LAG(status) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as prev_status,
        
        -- Next event details
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as next_event_timestamp,
        
        LEAD(location_id) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as next_location_id,
        
        -- Time calculations
        event_timestamp - LAG(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as time_since_prev_event,
        
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) - event_timestamp as time_to_next_event,
        
        -- Location-based aggregations
        COUNT(*) OVER (
            PARTITION BY shipment_id, location_id
        ) as events_at_location,
        
        -- Status duration calculations
        SUM(
            CASE WHEN status = 'in_transit' THEN 
                EXTRACT(EPOCH FROM (
                    LEAD(event_timestamp) OVER (
                        PARTITION BY shipment_id 
                        ORDER BY event_timestamp ASC
                    ) - event_timestamp
                )) / 3600.0  -- Hours in transit
            END
        ) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
            ROWS UNBOUNDED PRECEDING
        ) as cumulative_transit_hours,
        
        -- Delivery timeline metrics
        FIRST_VALUE(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
            ROWS UNBOUNDED PRECEDING
        ) as first_event_timestamp,
        
        CASE 
            WHEN status = 'delivered' THEN event_timestamp
            ELSE NULL
        END as delivery_timestamp,
        
        -- Add more window functions based on your specific analysis needs
        -- These might include:
        -- - Distance calculations between locations
        -- - Delay calculations vs expected timeline
        -- - Exception detection (stuck shipments, etc.)
        -- - Performance metrics per facility/carrier
        
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_timestamp,
        'daily_base' as process_type
        
    FROM enriched_data
)

-- Step 6: Final selection with additional derived fields
SELECT 
    *,
    
    -- Total shipment duration (if delivered)
    CASE 
        WHEN delivery_timestamp IS NOT NULL THEN
            EXTRACT(EPOCH FROM (delivery_timestamp - first_event_timestamp)) / 3600.0
        ELSE NULL
    END as total_shipment_hours,
    
    -- Shipment progress indicator
    CASE 
        WHEN status = 'delivered' THEN 100.0
        WHEN status = 'out_for_delivery' THEN 90.0
        WHEN status = 'in_transit' THEN 50.0
        WHEN status = 'picked_up' THEN 20.0
        ELSE 10.0
    END as progress_percentage,
    
    -- Add business logic calculations specific to your use case
    -- For example: SLA compliance, expected vs actual delivery time, etc.
    
    -- Indexing hint for performance
    shipment_id || '|' || event_sequence::text as timeline_key

FROM timeline_with_windows;

-- Step 7: Create indexes for performance
CREATE INDEX idx_shipment_timeline_base_shipment_id 
ON shipment_timeline_base(shipment_id);

CREATE INDEX idx_shipment_timeline_base_timestamp 
ON shipment_timeline_base(event_timestamp);

CREATE INDEX idx_shipment_timeline_base_location 
ON shipment_timeline_base(location_id);

CREATE INDEX idx_shipment_timeline_base_status 
ON shipment_timeline_base(status);

-- Composite index for common queries
CREATE INDEX idx_shipment_timeline_base_composite 
ON shipment_timeline_base(shipment_id, event_timestamp, status);

-- Step 8: Update checkpoint to indicate successful completion
UPDATE processing_checkpoints 
SET 
    last_processed_timestamp = CURRENT_DATE,
    last_update_time = CURRENT_TIMESTAMP,
    process_type = 'daily_base',
    status = 'completed'
WHERE checkpoint_id = 'shipment_timeline';

-- Step 9: Generate statistics for monitoring
INSERT INTO processing_stats (
    checkpoint_id,
    process_date,
    records_processed,
    processing_duration_seconds,
    process_type
)
SELECT 
    'shipment_timeline',
    CURRENT_DATE,
    COUNT(*),
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - (
        SELECT last_update_time 
        FROM processing_checkpoints 
        WHERE checkpoint_id = 'shipment_timeline'
    ))),
    'daily_base'
FROM shipment_timeline_base;

-- Analysis query to verify results
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT shipment_id) as unique_shipments,
    MIN(event_timestamp) as earliest_event,
    MAX(event_timestamp) as latest_event,
    COUNT(DISTINCT location_id) as unique_locations,
    AVG(events_at_location) as avg_events_per_location_per_shipment,
    COUNT(*) FILTER (WHERE delivery_timestamp IS NOT NULL) as delivered_shipments
FROM shipment_timeline_base;