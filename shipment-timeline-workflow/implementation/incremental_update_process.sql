-- Incremental Update Process for Shipment Timeline
-- Triggered by S3 events when new data arrives
-- Only processes NEW events since last checkpoint

-- Step 1: Get current checkpoint
CREATE TEMP TABLE current_checkpoint AS
SELECT 
    last_processed_timestamp,
    process_type,
    status
FROM processing_checkpoints 
WHERE checkpoint_id = 'shipment_timeline';

-- Verify we have a valid base to work from
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM current_checkpoint WHERE process_type = 'daily_base' AND status = 'completed') THEN
        RAISE EXCEPTION 'Daily base process must be completed before running incremental updates';
    END IF;
END $$;

-- Step 2: Create incremental timeline table if not exists
CREATE TABLE IF NOT EXISTS shipment_timeline_incremental (
    LIKE shipment_timeline_base INCLUDING ALL
);

-- Add processing metadata columns specific to incremental
ALTER TABLE shipment_timeline_incremental 
ADD COLUMN IF NOT EXISTS incremental_batch_id UUID DEFAULT gen_random_uuid(),
ADD COLUMN IF NOT EXISTS incremental_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Step 3: Extract only NEW events since checkpoint
CREATE TEMP TABLE new_events AS
WITH checkpoint AS (
    SELECT last_processed_timestamp FROM current_checkpoint
),

raw_new_events AS (
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
        created_date,
        updated_date
    FROM shipment_events  -- Adjust table name to match your schema
    WHERE event_timestamp > (SELECT last_processed_timestamp FROM checkpoint)
        AND status IS NOT NULL
        AND event_timestamp <= CURRENT_TIMESTAMP  -- Don't process future events
)
SELECT 
    rne.*,
    -- Enrich with location data
    loc.location_name,
    loc.region,
    loc.country_code,
    -- Enrich with package data
    pkg.package_weight,
    pkg.package_dimensions,
    pkg.service_level
FROM raw_new_events rne
LEFT JOIN locations loc ON rne.location_id = loc.location_id
LEFT JOIN packages pkg ON rne.package_id = pkg.package_id;

-- Step 4: Get context from base timeline for existing shipments
CREATE TEMP TABLE existing_shipment_context AS
SELECT 
    ne.shipment_id,
    -- Get the latest state from base timeline
    MAX(stb.event_sequence) as last_base_sequence,
    MAX(stb.event_timestamp) as last_base_timestamp,
    MAX(stb.location_id) as last_base_location_id,
    MAX(stb.status) as last_base_status,
    MAX(stb.first_event_timestamp) as shipment_first_event,
    MAX(stb.cumulative_transit_hours) as base_cumulative_transit_hours
FROM (SELECT DISTINCT shipment_id FROM new_events) ne
JOIN shipment_timeline_base stb ON ne.shipment_id = stb.shipment_id
WHERE stb.event_timestamp = (
    SELECT MAX(stb2.event_timestamp) 
    FROM shipment_timeline_base stb2 
    WHERE stb2.shipment_id = ne.shipment_id
)
GROUP BY ne.shipment_id;

-- Step 5: Process incremental events with window functions
CREATE TEMP TABLE processed_incremental AS
WITH incremental_with_context AS (
    SELECT 
        ne.*,
        esc.last_base_sequence,
        esc.last_base_timestamp,
        esc.last_base_location_id,
        esc.last_base_status,
        esc.shipment_first_event,
        esc.base_cumulative_transit_hours,
        
        -- For completely new shipments (not in base)
        CASE WHEN esc.shipment_id IS NULL THEN 'new_shipment' 
             ELSE 'existing_shipment' END as shipment_type
        
    FROM new_events ne
    LEFT JOIN existing_shipment_context esc ON ne.shipment_id = esc.shipment_id
),

-- Compute window functions for incremental data
windowed_incremental AS (
    SELECT 
        *,
        
        -- Event sequencing (continue from base)
        COALESCE(last_base_sequence, 0) + 
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as event_sequence,
        
        -- Previous event timestamp
        COALESCE(
            last_base_timestamp,  -- Use base if this is first incremental event
            LAG(event_timestamp) OVER (
                PARTITION BY shipment_id 
                ORDER BY event_timestamp ASC
            )
        ) as prev_event_timestamp,
        
        -- Previous location
        COALESCE(
            last_base_location_id,
            LAG(location_id) OVER (
                PARTITION BY shipment_id 
                ORDER BY event_timestamp ASC
            )
        ) as prev_location_id,
        
        -- Previous status
        COALESCE(
            last_base_status,
            LAG(status) OVER (
                PARTITION BY shipment_id 
                ORDER BY event_timestamp ASC
            )
        ) as prev_status,
        
        -- Next event details (within incremental batch only)
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as next_event_timestamp,
        
        LEAD(location_id) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) as next_location_id,
        
        -- Time calculations
        event_timestamp - COALESCE(
            last_base_timestamp,
            LAG(event_timestamp) OVER (
                PARTITION BY shipment_id 
                ORDER BY event_timestamp ASC
            )
        ) as time_since_prev_event,
        
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
        ) - event_timestamp as time_to_next_event,
        
        -- Location-based counts (incremental only - would need to join base for total)
        COUNT(*) OVER (
            PARTITION BY shipment_id, location_id
        ) as incremental_events_at_location,
        
        -- Transit time calculations (building on base)
        COALESCE(base_cumulative_transit_hours, 0) + 
        SUM(
            CASE WHEN status = 'in_transit' THEN 
                EXTRACT(EPOCH FROM (
                    COALESCE(
                        LEAD(event_timestamp) OVER (
                            PARTITION BY shipment_id 
                            ORDER BY event_timestamp ASC
                        ),
                        CURRENT_TIMESTAMP  -- If this is the latest event
                    ) - event_timestamp
                )) / 3600.0
            ELSE 0 END
        ) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp ASC
            ROWS UNBOUNDED PRECEDING
        ) as cumulative_transit_hours,
        
        -- First event timestamp (from base or this batch)
        COALESCE(
            shipment_first_event,
            FIRST_VALUE(event_timestamp) OVER (
                PARTITION BY shipment_id 
                ORDER BY event_timestamp ASC
                ROWS UNBOUNDED PRECEDING
            )
        ) as first_event_timestamp,
        
        -- Delivery detection
        CASE 
            WHEN status = 'delivered' THEN event_timestamp
            ELSE NULL
        END as delivery_timestamp,
        
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_timestamp,
        'incremental' as process_type
        
    FROM incremental_with_context
)

-- Final incremental processing with derived fields
SELECT 
    *,
    
    -- Events at location (combine base + incremental if needed)
    CASE 
        WHEN shipment_type = 'existing_shipment' THEN 
            incremental_events_at_location  -- Would need base data for true total
        ELSE incremental_events_at_location
    END as events_at_location,
    
    -- Total shipment duration (if delivered in this batch)
    CASE 
        WHEN delivery_timestamp IS NOT NULL THEN
            EXTRACT(EPOCH FROM (delivery_timestamp - first_event_timestamp)) / 3600.0
        ELSE NULL
    END as total_shipment_hours,
    
    -- Progress percentage
    CASE 
        WHEN status = 'delivered' THEN 100.0
        WHEN status = 'out_for_delivery' THEN 90.0
        WHEN status = 'in_transit' THEN 50.0
        WHEN status = 'picked_up' THEN 20.0
        ELSE 10.0
    END as progress_percentage,
    
    -- Timeline key
    shipment_id || '|' || event_sequence::text as timeline_key,
    
    -- Incremental processing metadata
    gen_random_uuid() as incremental_batch_id,
    CURRENT_TIMESTAMP as incremental_processed_at

FROM windowed_incremental;

-- Step 6: Insert processed incremental data
INSERT INTO shipment_timeline_incremental
SELECT * FROM processed_incremental;

-- Step 7: Update checkpoint with new timestamp
UPDATE processing_checkpoints 
SET 
    last_processed_timestamp = (
        SELECT MAX(event_timestamp) 
        FROM new_events
    ),
    last_update_time = CURRENT_TIMESTAMP,
    process_type = 'incremental',
    status = 'completed'
WHERE checkpoint_id = 'shipment_timeline';

-- Step 8: Log processing statistics
INSERT INTO processing_stats (
    checkpoint_id,
    process_date,
    records_processed,
    processing_duration_seconds,
    process_type,
    batch_id
)
SELECT 
    'shipment_timeline',
    CURRENT_DATE,
    COUNT(*),
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(incremental_processed_at))),
    'incremental',
    incremental_batch_id::text
FROM processed_incremental
GROUP BY incremental_batch_id;

-- Step 9: Cleanup old incremental data (optional)
-- Keep only last 24 hours of incremental data to prevent table bloat
DELETE FROM shipment_timeline_incremental 
WHERE incremental_processed_at < CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- Step 10: Analysis of incremental batch
SELECT 
    'INCREMENTAL_BATCH_SUMMARY' as summary_type,
    COUNT(*) as new_events_processed,
    COUNT(DISTINCT shipment_id) as shipments_affected,
    COUNT(*) FILTER (WHERE shipment_type = 'new_shipment') as completely_new_shipments,
    COUNT(*) FILTER (WHERE shipment_type = 'existing_shipment') as updates_to_existing,
    COUNT(*) FILTER (WHERE status = 'delivered') as deliveries_completed,
    MIN(event_timestamp) as earliest_new_event,
    MAX(event_timestamp) as latest_new_event,
    MAX(incremental_batch_id) as batch_id
FROM processed_incremental;