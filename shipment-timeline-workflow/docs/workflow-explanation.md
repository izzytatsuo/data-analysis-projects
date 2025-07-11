# Shipment Timeline Workflow - Detailed Explanation

## 🔄 The Two-Stage Processing Pattern

### Stage 1: Daily Base Process (Midnight UTC)

**Purpose**: Create a comprehensive baseline of shipment timeline data for the last 10 days.

**Process Flow**:
```sql
-- 1. Extract 10 days of shipment data
WITH base_shipments AS (
    SELECT 
        shipment_id,
        event_timestamp,
        event_type,
        location_id,
        status,
        -- Key fields for timeline analysis
    FROM shipment_events 
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL '10 days'
        AND event_timestamp < CURRENT_DATE
),

-- 2. Compute expensive window functions
timeline_with_windows AS (
    SELECT *,
        -- Timeline position calculations
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp
        ) as event_sequence,
        
        -- Time between events
        LAG(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp
        ) as prev_event_time,
        
        -- Status duration calculations  
        LEAD(event_timestamp) OVER (
            PARTITION BY shipment_id 
            ORDER BY event_timestamp
        ) as next_event_time,
        
        -- Complex aggregations
        COUNT(*) OVER (
            PARTITION BY shipment_id, location_id
        ) as location_event_count
        
    FROM base_shipments
)

-- 3. Store base timeline view
INSERT INTO shipment_timeline_base
SELECT * FROM timeline_with_windows;

-- 4. Update checkpoint
UPDATE processing_checkpoints 
SET last_processed_timestamp = CURRENT_DATE,
    process_type = 'daily_base'
WHERE checkpoint_id = 'shipment_timeline';
```

**Key Characteristics**:
- ✅ **Complete**: Processes all data for the time window
- ⚠️ **Expensive**: Full window function computation
- 🕐 **Scheduled**: Runs once daily at midnight UTC
- 💾 **Persistent**: Results stored for incremental updates

---

### Stage 2: Incremental Updates (Event-Driven)

**Purpose**: Append only new shipment events without reprocessing historical data.

**Trigger**: S3 event when new data files arrive

**Process Flow**:
```sql
-- 1. Get checkpoint - what's been processed already
WITH checkpoint AS (
    SELECT last_processed_timestamp 
    FROM processing_checkpoints 
    WHERE checkpoint_id = 'shipment_timeline'
),

-- 2. Extract ONLY new events since checkpoint
new_events AS (
    SELECT 
        shipment_id,
        event_timestamp,
        event_type,
        location_id,
        status
    FROM shipment_events 
    WHERE event_timestamp > (SELECT last_processed_timestamp FROM checkpoint)
),

-- 3. For existing shipments, get their latest state from base
existing_shipment_context AS (
    SELECT DISTINCT
        ne.shipment_id,
        stb.event_sequence as last_base_sequence,
        stb.event_timestamp as last_base_timestamp
    FROM new_events ne
    JOIN shipment_timeline_base stb ON ne.shipment_id = stb.shipment_id
    WHERE stb.event_timestamp = (
        SELECT MAX(event_timestamp) 
        FROM shipment_timeline_base stb2 
        WHERE stb2.shipment_id = ne.shipment_id
    )
),

-- 4. Compute window functions ONLY for new events
incremental_timeline AS (
    SELECT 
        ne.*,
        
        -- Continue sequence from base
        COALESCE(esc.last_base_sequence, 0) + 
        ROW_NUMBER() OVER (
            PARTITION BY ne.shipment_id 
            ORDER BY ne.event_timestamp
        ) as event_sequence,
        
        -- Previous event time (from base or new events)
        COALESCE(
            esc.last_base_timestamp,
            LAG(ne.event_timestamp) OVER (
                PARTITION BY ne.shipment_id 
                ORDER BY ne.event_timestamp
            )
        ) as prev_event_time,
        
        -- Other window calculations for new events only
        LEAD(ne.event_timestamp) OVER (
            PARTITION BY ne.shipment_id 
            ORDER BY ne.event_timestamp
        ) as next_event_time
        
    FROM new_events ne
    LEFT JOIN existing_shipment_context esc ON ne.shipment_id = esc.shipment_id
)

-- 5. Append to timeline (not replace)
INSERT INTO shipment_timeline_incremental
SELECT * FROM incremental_timeline;

-- 6. Update checkpoint
UPDATE processing_checkpoints 
SET last_processed_timestamp = (SELECT MAX(event_timestamp) FROM new_events),
    last_update_time = CURRENT_TIMESTAMP,
    process_type = 'incremental'
WHERE checkpoint_id = 'shipment_timeline';
```

**Key Characteristics**:
- ⚡ **Fast**: Only processes new data
- 💰 **Cost-effective**: Minimal compute resources
- 🔄 **Event-driven**: Triggers automatically on data arrival
- 🧩 **Contextual**: Maintains continuity with base data

---

## 🎯 Query Strategy for Real-Time Views

When users need current timeline data, combine base + incremental:

```sql
-- Real-time shipment timeline view
WITH combined_timeline AS (
    -- Get base data (bulk of historical timeline)
    SELECT *, 'base' as source_type
    FROM shipment_timeline_base
    
    UNION ALL
    
    -- Add incremental updates
    SELECT *, 'incremental' as source_type  
    FROM shipment_timeline_incremental
),

-- Final timeline with proper ordering
final_timeline AS (
    SELECT 
        shipment_id,
        event_timestamp,
        event_type,
        location_id,
        status,
        event_sequence,
        prev_event_time,
        next_event_time,
        source_type,
        
        -- Recompute any cross-boundary calculations if needed
        event_timestamp - prev_event_time as time_since_prev_event
        
    FROM combined_timeline
    ORDER BY shipment_id, event_timestamp
)

SELECT * FROM final_timeline;
```

## 📊 Performance Benefits

| Aspect | Full Reprocessing | Incremental Pattern |
|--------|------------------|-------------------|
| **Daily Compute Time** | 4-6 hours | 15-30 minutes |
| **Intraday Updates** | 2-4 hours each | 2-5 minutes each |
| **Cost per Update** | $50-100 | $2-5 |
| **Data Freshness** | 4-6 hour lag | 5-10 minute lag |
| **Resource Usage** | 100% dataset | 1-5% dataset |

## 🔧 Implementation Considerations

### Checkpoint Management
- **Atomic Updates**: Ensure checkpoint updates are transactional
- **Failure Recovery**: Handle partial processing scenarios
- **Time Zone Consistency**: All timestamps in UTC

### Data Consistency
- **Late Arriving Data**: Handle events that arrive out of order
- **Duplicate Detection**: Ensure idempotency in incremental updates
- **Schema Evolution**: Handle changes in event structure

### Monitoring & Alerting
- **Processing Lag**: Alert if incremental updates fall behind
- **Data Quality**: Validate window function results
- **Checkpoint Health**: Monitor checkpoint advancement

---

**Next**: See `implementation/` for actual SQL code and `architecture/` for system diagrams.