-- Integrated Timeline Queries
-- These queries combine container planning data with shipment timeline events

-- ---------------------------------------------------------
-- 1. Create location mapping table (standard setup)
-- ---------------------------------------------------------

DROP TABLE IF EXISTS amzl_mapping;
CREATE TEMP TABLE amzl_mapping AS (
    SELECT
        location_id AS node,
        timezone,
        region,
        country,
        GETDATE() AS run_time_utc,
        CAST(CAST(GETDATE() AS DATE) AS TIMESTAMP) AS day_start_utc,
        CONVERT_TIMEZONE('UTC', timezone, GETDATE()) AS run_time_local,
        CONVERT_TIMEZONE('UTC', timezone, CAST(CAST(GETDATE() AS DATE) AS TIMESTAMP)) AS local_offset_utc
    FROM
        "amzlanalytics"."perfectmile"."d_perfectmile_node_mapping_mdm"
    WHERE
        1=1
        AND location_status = 'A'
        AND country IN('US', 'CA')
        -- Uncomment to filter by specific regions if needed
        -- AND UPPER(region) IN('ROCKIES', 'UPSTATE NY') 
        AND (location_type = 'DS' 
            OR location_id IN ('MCO5', 'ZYG1', 'ZYN9', 'XVV2', 'XVV3', 'XYT6', 'XLC1', 'XVC1', 'XNK2')
        )
);

-- ---------------------------------------------------------
-- 2. Integrated Timeline View
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_integrated_timeline AS
WITH 
-- Get base shipment timeline events
timeline_events AS (
    SELECT 
        tracking_id,
        shipment_key,
        status_code,
        status_description,
        status_time AS event_time,
        source,
        status_level_tertiary,
        status_level_secondary,
        status_level_primary,
        -- Extract potential location from description
        REGEXP_SUBSTR(status_description, '[A-Z0-9]{3,5}') AS potential_location
    FROM 
        amzlcore.d_shipment_timeline
),

-- Get container planning events transformed into timeline format
container_events AS (
    SELECT
        cp.tracking_id,
        cp.shipment_id AS shipment_key,
        CASE
            WHEN event_type = 'PLAN' THEN 'CONTAINER_PLANNED'
            WHEN event_type = 'INDUCT' THEN 'CONTAINER_INDUCTED'
            WHEN event_type = 'STOW' THEN 'CONTAINER_STOWED'
        END AS status_code,
        CASE
            WHEN event_type = 'PLAN' THEN 'Container Planning Completed at ' || station_code
            WHEN event_type = 'INDUCT' THEN 'Package Inducted at ' || station_code || ' - ' || COALESCE(induct_sort_zone, 'Unknown Zone')
            WHEN event_type = 'STOW' THEN 'Package Stowed at ' || station_code || ' in ' || COALESCE(stow_sort_zone, 'Unknown Zone')
        END AS status_description,
        CASE
            WHEN event_type = 'PLAN' THEN cp.dcap_run_time_local
            WHEN event_type = 'INDUCT' THEN cp.induct_datetime_local
            WHEN event_type = 'STOW' THEN cp.stow_datetime
        END AS event_time,
        'CONTAINER_PLANNING' AS source,
        'CONTAINER' AS status_level_tertiary,
        CASE
            WHEN event_type = 'PLAN' THEN 'PLANNING'
            WHEN event_type = 'INDUCT' THEN 'INDUCTION'
            WHEN event_type = 'STOW' THEN 'STOWING'
        END AS status_level_secondary,
        'DELIVERY_STATION' AS status_level_primary,
        cp.station_code AS potential_location,
        cp.is_planned,
        cp.is_inducted,
        cp.is_inducted_as_planned,
        cp.originating_node,
        cp.originating_fc_or_sc,
        -- Add planning status flags
        CASE WHEN cp.is_inducted_not_planned = 1 THEN 'NO_CYCLE' ELSE 'NORMAL' END AS planning_status,
        -- Add timezone information
        map.timezone,
        map.region,
        map.country
    FROM 
        amzlcore.d_container_planning_events cp
    CROSS JOIN (
        VALUES ('PLAN'), ('INDUCT'), ('STOW')
    ) AS events(event_type)
    LEFT JOIN
        amzl_mapping map ON cp.station_code = map.node
    WHERE
        -- Only include events that actually happened
        (event_type = 'PLAN' AND cp.dcap_run_time_local IS NOT NULL) OR
        (event_type = 'INDUCT' AND cp.induct_datetime_local IS NOT NULL AND cp.is_inducted = 1) OR
        (event_type = 'STOW' AND cp.stow_datetime IS NOT NULL)
)

-- Union all events together to create integrated timeline
SELECT
    te.tracking_id,
    te.shipment_key,
    te.status_code,
    te.status_description,
    te.event_time,
    te.source,
    te.status_level_tertiary,
    te.status_level_secondary,
    te.status_level_primary,
    te.potential_location,
    NULL AS is_planned,
    NULL AS is_inducted,
    NULL AS is_inducted_as_planned,
    NULL AS originating_node,
    NULL AS originating_fc_or_sc,
    NULL AS planning_status,
    map.timezone,
    map.region,
    map.country,
    -- Convert to local timezone when available
    CASE 
        WHEN map.timezone IS NOT NULL THEN CONVERT_TIMEZONE('UTC', map.timezone, te.event_time)
        ELSE te.event_time
    END AS event_time_local
FROM
    timeline_events te
LEFT JOIN
    amzl_mapping map ON te.potential_location = map.node

UNION ALL

SELECT
    ce.tracking_id,
    ce.shipment_key,
    ce.status_code,
    ce.status_description,
    ce.event_time,
    ce.source,
    ce.status_level_tertiary,
    ce.status_level_secondary,
    ce.status_level_primary,
    ce.potential_location,
    ce.is_planned,
    ce.is_inducted,
    ce.is_inducted_as_planned,
    ce.originating_node,
    ce.originating_fc_or_sc,
    ce.planning_status,
    ce.timezone,
    ce.region,
    ce.country,
    -- Convert to local timezone when available
    CASE 
        WHEN ce.timezone IS NOT NULL THEN CONVERT_TIMEZONE('UTC', ce.timezone, ce.event_time)
        ELSE ce.event_time
    END AS event_time_local
FROM
    container_events ce

ORDER BY
    tracking_id,
    event_time;

-- ---------------------------------------------------------
-- 3. End-to-End Package Journey Query
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_package_journey AS
WITH 
-- Get all events for each package
package_events AS (
    SELECT 
        tracking_id,
        status_code,
        status_description,
        event_time,
        event_time_local,
        source,
        planning_status,
        originating_node,
        region,
        country
    FROM 
        amzlcore.v_integrated_timeline
),

-- Get first and last events for each package
package_boundaries AS (
    SELECT
        tracking_id,
        MIN(event_time) AS first_event_time,
        MAX(event_time) AS last_event_time,
        -- Count events by source
        COUNT(CASE WHEN source = 'SLAM_LEG' THEN 1 END) AS slam_events,
        COUNT(CASE WHEN source = 'GMP' THEN 1 END) AS gmp_events,
        COUNT(CASE WHEN source = 'PSE' THEN 1 END) AS pse_events,
        COUNT(CASE WHEN source = 'CONTAINER_PLANNING' THEN 1 END) AS container_events,
        -- Check for no-cycle
        MAX(CASE WHEN planning_status = 'NO_CYCLE' THEN 1 ELSE 0 END) AS has_no_cycle
    FROM
        package_events
    GROUP BY
        tracking_id
),

-- Get key milestones for each package
package_milestones AS (
    SELECT
        pe.tracking_id,
        -- First SLAM event
        MIN(CASE WHEN pe.source = 'SLAM_LEG' THEN pe.event_time END) AS first_slam_time,
        -- First container planning event
        MIN(CASE WHEN pe.status_code = 'CONTAINER_PLANNED' THEN pe.event_time END) AS planning_time,
        -- First induction event
        MIN(CASE WHEN pe.status_code = 'CONTAINER_INDUCTED' THEN pe.event_time END) AS induction_time,
        -- First stow event
        MIN(CASE WHEN pe.status_code = 'CONTAINER_STOWED' THEN pe.event_time END) AS stow_time,
        -- Last event (delivery or final status)
        MAX(pe.event_time) AS final_event_time,
        -- Final status
        FIRST_VALUE(pe.status_code) OVER (
            PARTITION BY pe.tracking_id 
            ORDER BY pe.event_time DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS final_status,
        -- First originating node
        MIN(pe.originating_node) KEEP (DENSE_RANK FIRST ORDER BY pe.event_time) AS first_origin_node,
        -- Region of final event
        MAX(pe.region) KEEP (DENSE_RANK LAST ORDER BY pe.event_time) AS final_region
    FROM
        package_events pe
    GROUP BY
        pe.tracking_id
)

-- Combine boundaries and milestones for complete journey view
SELECT
    pb.tracking_id,
    pb.first_event_time,
    pb.last_event_time,
    EXTRACT(EPOCH FROM (pb.last_event_time - pb.first_event_time))/3600 AS journey_hours,
    pb.slam_events,
    pb.gmp_events,
    pb.pse_events,
    pb.container_events,
    pb.has_no_cycle,
    -- Planning timing
    pm.planning_time,
    pm.induction_time,
    pm.stow_time,
    CASE 
        WHEN pm.planning_time IS NULL AND pm.induction_time IS NOT NULL THEN 'NO_CYCLE'
        WHEN pm.planning_time IS NOT NULL AND pm.induction_time IS NOT NULL THEN
            CASE 
                WHEN pm.planning_time <= pm.induction_time THEN 'NORMAL'
                ELSE 'TIMING_ANOMALY'
            END
        ELSE 'INCOMPLETE'
    END AS planning_pattern,
    -- Time between milestones in minutes
    EXTRACT(EPOCH FROM (pm.induction_time - pm.planning_time))/60 AS plan_to_induct_mins,
    EXTRACT(EPOCH FROM (pm.stow_time - pm.induction_time))/60 AS induct_to_stow_mins,
    -- Journey origin and destination
    pm.first_origin_node,
    pm.final_region,
    pm.final_status
FROM
    package_boundaries pb
JOIN
    package_milestones pm ON pb.tracking_id = pm.tracking_id
ORDER BY
    pb.first_event_time DESC;

-- ---------------------------------------------------------
-- 4. Planning Effectiveness by Date and Station
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_planning_effectiveness AS
WITH 
-- Get unique packages with their planning status
package_planning AS (
    SELECT
        tracking_id,
        MIN(event_time::DATE) AS event_date,
        MAX(CASE WHEN source = 'CONTAINER_PLANNING' THEN potential_location END) AS station_code,
        MAX(CASE WHEN status_code = 'CONTAINER_PLANNED' THEN 1 ELSE 0 END) AS was_planned,
        MAX(CASE WHEN status_code = 'CONTAINER_INDUCTED' THEN 1 ELSE 0 END) AS was_inducted,
        MAX(CASE WHEN planning_status = 'NO_CYCLE' THEN 1 ELSE 0 END) AS is_no_cycle,
        MAX(region) AS region,
        MAX(country) AS country
    FROM 
        amzlcore.v_integrated_timeline
    GROUP BY
        tracking_id
)

-- Summarize by date and station
SELECT
    event_date,
    station_code,
    region,
    country,
    COUNT(*) AS total_packages,
    SUM(was_planned) AS planned_packages,
    SUM(was_inducted) AS inducted_packages,
    SUM(CASE WHEN was_planned = 1 AND was_inducted = 1 THEN 1 ELSE 0 END) AS planned_and_inducted,
    SUM(CASE WHEN was_planned = 0 AND was_inducted = 1 THEN 1 ELSE 0 END) AS no_cycle_packages,
    -- Calculate percentages
    ROUND(100.0 * SUM(was_planned) / NULLIF(COUNT(*), 0), 2) AS planning_rate,
    ROUND(100.0 * SUM(CASE WHEN was_planned = 0 AND was_inducted = 1 THEN 1 ELSE 0 END) / 
          NULLIF(SUM(was_inducted), 0), 2) AS no_cycle_rate
FROM
    package_planning
GROUP BY
    event_date, station_code, region, country
ORDER BY
    event_date DESC, no_cycle_rate DESC;

-- ---------------------------------------------------------
-- 5. Timeline Gap Analysis
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_timeline_gaps AS
WITH 
-- Prepare events with row numbers for sequential analysis
sequential_events AS (
    SELECT
        tracking_id,
        status_code,
        status_description,
        event_time,
        source,
        -- Assign row numbers to order events chronologically
        ROW_NUMBER() OVER (PARTITION BY tracking_id ORDER BY event_time) AS event_seq
    FROM 
        amzlcore.v_integrated_timeline
),

-- Join events to their next event to find gaps
event_pairs AS (
    SELECT
        curr.tracking_id,
        curr.status_code AS curr_status,
        curr.event_time AS curr_time,
        curr.source AS curr_source,
        next.status_code AS next_status,
        next.event_time AS next_time,
        next.source AS next_source,
        -- Calculate time difference in hours
        EXTRACT(EPOCH FROM (next.event_time - curr.event_time))/3600 AS hours_between
    FROM
        sequential_events curr
    JOIN
        sequential_events next ON curr.tracking_id = next.tracking_id AND curr.event_seq = next.event_seq - 1
)

-- Find significant gaps (more than 6 hours between events)
SELECT
    tracking_id,
    curr_status,
    curr_time,
    curr_source,
    next_status,
    next_time,
    next_source,
    hours_between,
    -- Categorize gap types
    CASE
        WHEN curr_source = 'CONTAINER_PLANNING' AND next_source != 'CONTAINER_PLANNING' THEN 'CONTAINER_TO_OTHER'
        WHEN curr_source != 'CONTAINER_PLANNING' AND next_source = 'CONTAINER_PLANNING' THEN 'OTHER_TO_CONTAINER'
        WHEN curr_source = 'CONTAINER_PLANNING' AND next_source = 'CONTAINER_PLANNING' THEN 'WITHIN_CONTAINER'
        ELSE 'WITHIN_TIMELINE'
    END AS gap_type
FROM
    event_pairs
WHERE
    hours_between > 6 -- Only gaps over 6 hours
ORDER BY
    hours_between DESC;

-- ---------------------------------------------------------
-- 6. Package Sample Query for Testing
-- ---------------------------------------------------------

-- This query retrieves a complete journey for a sample package
-- to test the integration between systems

-- Sample usage: 
-- SELECT * FROM amzlcore.fn_get_package_journey('TBA321830680678')

CREATE OR REPLACE FUNCTION amzlcore.fn_get_package_journey(p_tracking_id VARCHAR)
RETURNS TABLE (
    event_seq INT,
    status_code VARCHAR,
    status_description VARCHAR,
    event_time TIMESTAMP,
    event_time_local TIMESTAMP,
    source VARCHAR,
    planning_status VARCHAR,
    hours_since_first NUMERIC,
    hours_until_next NUMERIC,
    has_anomaly INT
) AS $$
BEGIN
    RETURN QUERY
    WITH package_timeline AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY event_time) AS event_seq,
            status_code,
            status_description,
            event_time,
            event_time_local,
            source,
            planning_status,
            -- Calculate hours since first event
            EXTRACT(EPOCH FROM (event_time - MIN(event_time) OVER ()))/3600 AS hours_since_first,
            -- Calculate hours until next event
            EXTRACT(EPOCH FROM (LEAD(event_time) OVER (ORDER BY event_time) - event_time))/3600 AS hours_until_next,
            -- Identify anomalies (e.g., induction before planning)
            CASE
                WHEN status_code = 'CONTAINER_INDUCTED' AND 
                     NOT EXISTS (SELECT 1 FROM amzlcore.v_integrated_timeline t2 
                                 WHERE t2.tracking_id = t1.tracking_id 
                                 AND t2.status_code = 'CONTAINER_PLANNED'
                                 AND t2.event_time <= t1.event_time) 
                THEN 1
                ELSE 0
            END AS has_anomaly
        FROM
            amzlcore.v_integrated_timeline t1
        WHERE
            tracking_id = p_tracking_id
    )
    
    SELECT
        event_seq,
        status_code,
        status_description,
        event_time,
        event_time_local,
        source,
        planning_status,
        hours_since_first,
        hours_until_next,
        has_anomaly
    FROM
        package_timeline
    ORDER BY
        event_seq;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------
-- 7. Regional Aggregation View
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_regional_planning_metrics AS
WITH 
-- Daily metrics by station
daily_metrics AS (
    SELECT
        event_date,
        station_code,
        region,
        country,
        total_packages,
        planned_packages,
        inducted_packages,
        no_cycle_packages,
        planning_rate,
        no_cycle_rate
    FROM 
        amzlcore.v_planning_effectiveness
)

-- Aggregate to region level
SELECT
    event_date,
    region,
    country,
    COUNT(DISTINCT station_code) AS station_count,
    SUM(total_packages) AS total_packages,
    SUM(planned_packages) AS planned_packages,
    SUM(inducted_packages) AS inducted_packages,
    SUM(no_cycle_packages) AS no_cycle_packages,
    -- Calculate volume-weighted averages
    ROUND(SUM(planning_rate * total_packages) / NULLIF(SUM(total_packages), 0), 2) AS weighted_planning_rate,
    ROUND(SUM(no_cycle_rate * inducted_packages) / NULLIF(SUM(inducted_packages), 0), 2) AS weighted_no_cycle_rate,
    -- Calculate standard deviation to measure regional consistency
    STDDEV_POP(no_cycle_rate) AS no_cycle_rate_stddev,
    -- Min/Max station performances
    MIN(no_cycle_rate) AS min_no_cycle_rate,
    MAX(no_cycle_rate) AS max_no_cycle_rate
FROM
    daily_metrics
GROUP BY
    event_date, region, country
ORDER BY
    event_date DESC, weighted_no_cycle_rate DESC;

-- ---------------------------------------------------------
-- 8. Originating Node Impact Analysis
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_originating_node_impact AS
WITH 
-- Get planning data with originating node information
origin_planning AS (
    SELECT
        tracking_id,
        MIN(event_time::DATE) AS event_date,
        MAX(CASE WHEN status_code = 'CONTAINER_INDUCTED' THEN potential_location END) AS station_code,
        MAX(originating_node) AS originating_node,
        MAX(originating_fc_or_sc) AS origin_type,
        MAX(CASE WHEN status_code = 'CONTAINER_PLANNED' THEN 1 ELSE 0 END) AS was_planned,
        MAX(CASE WHEN status_code = 'CONTAINER_INDUCTED' THEN 1 ELSE 0 END) AS was_inducted,
        MAX(CASE WHEN planning_status = 'NO_CYCLE' THEN 1 ELSE 0 END) AS is_no_cycle,
        MAX(region) AS dest_region
    FROM 
        amzlcore.v_integrated_timeline
    WHERE
        originating_node IS NOT NULL
    GROUP BY
        tracking_id
)

-- Aggregate by originating node and destination station
SELECT
    originating_node,
    origin_type,
    station_code,
    dest_region,
    COUNT(*) AS package_count,
    SUM(was_planned) AS planned_packages,
    SUM(was_inducted) AS inducted_packages,
    SUM(is_no_cycle) AS no_cycle_packages,
    -- Calculate rates
    ROUND(100.0 * SUM(was_planned) / COUNT(*), 2) AS planning_rate,
    ROUND(100.0 * SUM(is_no_cycle) / NULLIF(SUM(was_inducted), 0), 2) AS no_cycle_rate,
    -- Flag problematic node-station pairs (high volume, high no-cycle)
    CASE
        WHEN COUNT(*) >= 100 AND (100.0 * SUM(is_no_cycle) / NULLIF(SUM(was_inducted), 0)) > 30 
        THEN 1 ELSE 0
    END AS is_problem_pair
FROM
    origin_planning
GROUP BY
    originating_node, origin_type, station_code, dest_region
HAVING
    COUNT(*) >= 10 -- Only pairs with meaningful volume
ORDER BY
    is_problem_pair DESC, package_count DESC;