# Shipment Timeline Project Session Summary

## Overview

This session focused on creating a comprehensive data model for tracking package shipments through multiple systems using a timeline-based approach in Redshift. The primary goal was to develop a consolidated view that brings together data from SLAM (Sort, Label, and Manifest), GMP (Global Marketplace Platform), and PSE (Package Systems Events) into a unified timeline structure.

## Key Design Decisions

### UNION ALL Approach
Instead of joining the three source tables on common keys (which would place related events on the same row), we implemented a UNION ALL approach that:
- Maintains each event as a separate row
- Preserves chronological ordering of events
- Enables timeline-based analysis across systems
- Follows the pattern used in waterfall_reference.sql

### Data Filtering Strategy
We implemented filtering to ensure we only analyze volume included in the slam_leg table:
- GMP records are filtered to only include those with matching shipment_key in slam_leg
- PSE records are filtered to only include those with matching tracking_id in GMP that also matches slam_leg
- This ensures consistent volume analysis across all three systems

### Common Key Structure
- `pk`: Common identifier (shipment_key or tracking_id) to track packages across systems
- `source_table`: Identifies which system the event came from ('slam_leg', 'GMP', or 'PSE')
- `event_timestamp`: Normalized timestamp for chronological ordering
- `shipment_id`, `package_id`, `tracking_id`, `dw_created_time`: Normalized across sources
- Preserved pse.package_id as a separate field (pse_package_id) to maintain system-specific IDs

### Optimization
- `SORTKEY(pk, event_timestamp)`: Optimizes for queries filtering by package and ordering by time
- `DISTKEY(pk)`: Distributes data efficiently across Redshift nodes

## SQL Implementation Highlights

### Consolidated Table Creation
```sql
CREATE TEMP TABLE consolidated SORTKEY(pk, event_timestamp) DISTKEY(pk) AS (
    -- SLAM_LEG events
    SELECT ... FROM slam_leg
    
    UNION ALL
    
    -- GMP events - filtered to match slam_leg
    SELECT ... FROM gmp
    WHERE EXISTS (
        SELECT 1 FROM slam_leg sl
        WHERE sl.shipment_key = g.shipment_key
    )
    
    UNION ALL
    
    -- PSE events - filtered to match GMP that matches slam_leg
    SELECT ... FROM pse
    WHERE EXISTS (
        SELECT 1 FROM gmp g
        JOIN slam_leg sl ON sl.shipment_key = g.shipment_key
        WHERE g.tracking_id = p.forward_tracking_id
    )
);
```

### Example Analytical Queries
We created example queries demonstrating how to:
1. Generate chronological event timelines for packages
2. Calculate event counts by source system
3. Find packages with delivery events
4. Analyze shipment status progression
5. Calculate time between key status transitions

## Decisions Regarding Window Functions

We considered implementing window functions similar to waterfall_reference.sql:
```sql
LAST_VALUE(ST."zone") IGNORE NULLS OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_slammed_zone
```

However, we decided not to include these window functions in the initial implementation:
- For simplicity and clarity of the base data model
- These can be added in future versions or directly in analytical queries as needed
- Status identification is now handled using CASE statements in queries

## UNION ALL vs JOIN Comparison

We evaluated both approaches:

### UNION ALL (Implemented)
- **Structure**: Events as separate rows, stacked vertically
- **Advantages**: Better for timeline analysis, status transitions, chronological order
- **Considerations**: More rows with sparse data (many NULL fields)

### JOIN (Alternative)
- **Structure**: Events combined into wide rows with many columns
- **Advantages**: Better for retrieving all information in a single row
- **Considerations**: More complex to analyze event sequences, potential duplicate events

## Future Enhancements

Potential improvements for future iterations:
1. Add window functions for latest values across events if needed
2. Create tertiary_status classification to normalize status across systems
3. Implement delivery tracking with was_delivered flag
4. Add primary/secondary status classification based on tertiary_status
5. Consider partition pruning strategies for large datasets
6. Explore materialized views for common analytics patterns