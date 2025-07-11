-- Container Planning Sample Queries with Location Mapping Integration
-- These queries integrate location mapping data with container planning analysis

-- ---------------------------------------------------------
-- 1. Create location mapping table (from d_shipment_timeline.ipynb)
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
-- 2. Integrate location mapping with container planning data
-- ---------------------------------------------------------

-- Create a temporary table with timezone-adjusted container planning data
DROP TABLE IF EXISTS container_planning_with_tz;
CREATE TEMP TABLE container_planning_with_tz AS (
    SELECT
        cp.*,
        map.timezone,
        map.region,
        map.country,
        -- Convert UTC timestamps to local timestamps using station timezone
        CONVERT_TIMEZONE('UTC', map.timezone, cp.dcap_run_time_local) AS dcap_time_local,
        CONVERT_TIMEZONE('UTC', map.timezone, cp.induct_datetime_local) AS induct_time_local,
        CONVERT_TIMEZONE('UTC', map.timezone, cp.stow_datetime) AS stow_time_local,
        CONVERT_TIMEZONE('UTC', map.timezone, cp.slam_datetime_local) AS slam_time_local,
        CONVERT_TIMEZONE('UTC', map.timezone, cp.actual_ds_arrival_datetime_local) AS arrival_time_local,
        CONVERT_TIMEZONE('UTC', map.timezone, cp.promised_arrival_datetime) AS promised_time_local,
        -- Extract date parts for easier aggregation
        DATE(CONVERT_TIMEZONE('UTC', map.timezone, cp.dcap_run_time_local)) AS dcap_date_local,
        DATE(CONVERT_TIMEZONE('UTC', map.timezone, cp.induct_datetime_local)) AS induct_date_local,
        EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', map.timezone, cp.induct_datetime_local)) AS induct_hour_local
    FROM
        amzlcore.d_container_planning_events cp
    LEFT JOIN
        amzl_mapping map ON cp.station_code = map.node
);

-- ---------------------------------------------------------
-- 3. Regional analysis of planning effectiveness
-- ---------------------------------------------------------

-- Planning effectiveness by region
SELECT
    region,
    country,
    COUNT(*) AS total_packages,
    SUM(is_planned) AS planned_packages,
    SUM(is_inducted) AS inducted_packages,
    SUM(is_inducted_as_planned) AS inducted_as_planned,
    SUM(is_inducted_not_planned) AS inducted_not_planned,
    ROUND(SUM(is_planned)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS planning_rate,
    ROUND(SUM(is_inducted_as_planned)::FLOAT / NULLIF(SUM(is_planned), 0) * 100, 1) AS execution_rate,
    COUNT(DISTINCT station_code) AS station_count
FROM
    container_planning_with_tz
GROUP BY
    region, country
ORDER BY
    total_packages DESC;

-- ---------------------------------------------------------
-- 4. Hourly induction pattern analysis
-- ---------------------------------------------------------

-- Analyze induction patterns by hour of day
SELECT
    region,
    station_code,
    induct_hour_local,
    COUNT(*) AS package_count,
    SUM(is_inducted_as_planned) AS inducted_as_planned,
    ROUND(SUM(is_inducted_as_planned)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS planning_compliance,
    AVG(EXTRACT(EPOCH FROM (stow_time_local - induct_time_local))/60) AS avg_induct_to_stow_mins
FROM
    container_planning_with_tz
WHERE
    induct_datetime_local IS NOT NULL
GROUP BY
    region, station_code, induct_hour_local
ORDER BY
    region, station_code, induct_hour_local;

-- ---------------------------------------------------------
-- 5. Cross-timezone analysis of originating node to destination
-- ---------------------------------------------------------

-- Analyze cross-timezone shipments
WITH origin_mapping AS (
    SELECT
        cp.tracking_id,
        cp.originating_node,
        cp.station_code AS destination_node,
        orig_map.timezone AS origin_timezone,
        dest_map.timezone AS destination_timezone,
        orig_map.region AS origin_region,
        dest_map.region AS destination_region,
        cp.slam_datetime_local AS slam_time_utc,
        cp.actual_ds_arrival_datetime_local AS arrival_time_utc,
        EXTRACT(EPOCH FROM (cp.actual_ds_arrival_datetime_local - cp.slam_datetime_local))/3600 AS transit_hours_utc,
        -- Calculate local times at origin and destination
        CONVERT_TIMEZONE('UTC', orig_map.timezone, cp.slam_datetime_local) AS slam_time_origin_local,
        CONVERT_TIMEZONE('UTC', dest_map.timezone, cp.actual_ds_arrival_datetime_local) AS arrival_time_dest_local,
        -- Calculate true transit time accounting for timezone differences
        EXTRACT(EPOCH FROM (
            CONVERT_TIMEZONE('UTC', dest_map.timezone, cp.actual_ds_arrival_datetime_local) - 
            CONVERT_TIMEZONE('UTC', orig_map.timezone, cp.slam_datetime_local)
        ))/3600 AS actual_transit_hours
    FROM
        amzlcore.d_container_planning_events cp
    LEFT JOIN
        amzl_mapping orig_map ON cp.originating_node = orig_map.node
    LEFT JOIN
        amzl_mapping dest_map ON cp.station_code = dest_map.node
    WHERE
        cp.slam_datetime_local IS NOT NULL
        AND cp.actual_ds_arrival_datetime_local IS NOT NULL
)

SELECT
    origin_region,
    destination_region,
    origin_timezone,
    destination_timezone,
    COUNT(*) AS package_count,
    AVG(transit_hours_utc) AS avg_transit_hours_utc,
    AVG(actual_transit_hours) AS avg_transit_hours_local_adjusted,
    -- Calculate the timezone adjustment impact
    AVG(actual_transit_hours - transit_hours_utc) AS avg_timezone_impact
FROM
    origin_mapping
WHERE
    origin_timezone != destination_timezone  -- Focus on cross-timezone shipments
GROUP BY
    origin_region, destination_region, origin_timezone, destination_timezone
ORDER BY
    package_count DESC;

-- ---------------------------------------------------------
-- 6. Local cutoff time analysis
-- ---------------------------------------------------------

-- Analyze meeting cutoff times in local timezone
SELECT
    cp.station_code,
    map.region,
    DATE(CONVERT_TIMEZONE('UTC', map.timezone, cp.station_arrival_cutoff_local)) AS cutoff_date_local,
    EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', map.timezone, cp.station_arrival_cutoff_local)) AS cutoff_hour_local,
    COUNT(*) AS package_count,
    SUM(CASE WHEN cp.actual_ds_arrival_datetime_local <= cp.station_arrival_cutoff_local THEN 1 ELSE 0 END) AS arrived_before_cutoff,
    ROUND(SUM(CASE WHEN cp.actual_ds_arrival_datetime_local <= cp.station_arrival_cutoff_local THEN 1 ELSE 0 END)::FLOAT / 
          NULLIF(COUNT(*), 0) * 100, 1) AS cutoff_compliance_rate,
    -- Average time relative to cutoff (negative means before cutoff)
    AVG(EXTRACT(EPOCH FROM (cp.actual_ds_arrival_datetime_local - cp.station_arrival_cutoff_local))/60) AS avg_mins_from_cutoff
FROM
    amzlcore.d_container_planning_events cp
JOIN
    amzl_mapping map ON cp.station_code = map.node
WHERE
    cp.station_arrival_cutoff_local IS NOT NULL
    AND cp.actual_ds_arrival_datetime_local IS NOT NULL
GROUP BY
    cp.station_code, map.region,
    DATE(CONVERT_TIMEZONE('UTC', map.timezone, cp.station_arrival_cutoff_local)),
    EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', map.timezone, cp.station_arrival_cutoff_local))
ORDER BY
    cutoff_compliance_rate;

-- ---------------------------------------------------------
-- 7. Integration with shipment timeline and location mapping
-- ---------------------------------------------------------

-- Create a comprehensive view of package journey with proper timezone handling
CREATE OR REPLACE VIEW amzlcore.v_package_journey_with_locations AS
WITH timeline_events AS (
    -- Get events from shipment timeline
    SELECT
        st.tracking_id,
        st.status_code,
        st.status_description,
        st.status_time AS event_time_utc,
        st.source,
        st.status_level_tertiary,
        st.status_level_secondary,
        st.status_level_primary,
        -- Extract location code from status description when available
        REGEXP_SUBSTR(st.status_description, '[A-Z0-9]{3,5}') AS potential_location_code
    FROM 
        amzlcore.d_shipment_timeline st
),
location_enhanced_timeline AS (
    -- Join events with location information
    SELECT
        te.*,
        COALESCE(
            -- Try to match with location from status description
            loc1.node,
            -- If no match, try to find location in container planning data
            (
                SELECT station_code 
                FROM amzlcore.d_container_planning_events 
                WHERE tracking_id = te.tracking_id
                LIMIT 1
            )
        ) AS location_code,
        COALESCE(loc1.timezone, loc2.timezone) AS event_timezone,
        COALESCE(loc1.region, loc2.region) AS event_region,
        COALESCE(loc1.country, loc2.country) AS event_country
    FROM 
        timeline_events te
    LEFT JOIN
        amzl_mapping loc1 ON te.potential_location_code = loc1.node
    LEFT JOIN
        amzl_mapping loc2 ON (
            SELECT station_code 
            FROM amzlcore.d_container_planning_events 
            WHERE tracking_id = te.tracking_id
            LIMIT 1
        ) = loc2.node
),
container_planning_events AS (
    -- Get container planning events with location data
    SELECT
        cp.tracking_id,
        'CONTAINER_' || 
        CASE 
            WHEN event_type = 'PLAN' THEN 'PLANNED'
            WHEN event_type = 'INDUCT' THEN 'INDUCTED'
            WHEN event_type = 'STOW' THEN 'STOWED'
        END AS status_code,
        CASE
            WHEN event_type = 'PLAN' THEN 'Container Planning Completed'
            WHEN event_type = 'INDUCT' THEN 'Package Inducted at ' || station_code
            WHEN event_type = 'STOW' THEN 'Package Stowed at ' || station_code || ' in ' || stow_sort_zone
        END AS status_description,
        CASE
            WHEN event_type = 'PLAN' THEN dcap_run_time_local
            WHEN event_type = 'INDUCT' THEN induct_datetime_local
            WHEN event_type = 'STOW' THEN stow_datetime
        END AS event_time_utc,
        'CONTAINER_PLANNING' AS source,
        'CONTAINER' AS status_level_tertiary,
        CASE
            WHEN event_type = 'PLAN' THEN 'PLANNING'
            WHEN event_type = 'INDUCT' THEN 'INDUCTION'
            WHEN event_type = 'STOW' THEN 'STOWING'
        END AS status_level_secondary,
        'DELIVERY_STATION' AS status_level_primary,
        cp.station_code AS location_code,
        map.timezone AS event_timezone,
        map.region AS event_region,
        map.country AS event_country
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

-- Combine timeline events and container planning events
SELECT 
    tracking_id,
    status_code,
    status_description,
    event_time_utc,
    -- Convert to local time based on location timezone when available
    CASE WHEN event_timezone IS NOT NULL 
         THEN CONVERT_TIMEZONE('UTC', event_timezone, event_time_utc) 
         ELSE event_time_utc 
    END AS event_time_local,
    source,
    status_level_tertiary,
    status_level_secondary,
    status_level_primary,
    location_code,
    event_timezone,
    event_region,
    event_country
FROM location_enhanced_timeline

UNION ALL

SELECT 
    tracking_id,
    status_code,
    status_description,
    event_time_utc,
    CASE WHEN event_timezone IS NOT NULL 
         THEN CONVERT_TIMEZONE('UTC', event_timezone, event_time_utc) 
         ELSE event_time_utc 
    END AS event_time_local,
    source,
    status_level_tertiary,
    status_level_secondary,
    status_level_primary,
    location_code,
    event_timezone,
    event_region,
    event_country
FROM container_planning_events

ORDER BY
    tracking_id,
    event_time_utc;

-- ---------------------------------------------------------
-- 8. Sample queries for common analyses
-- ---------------------------------------------------------

-- 8.1 Find packages crossing time zones with timeline anomalies
SELECT 
    t1.tracking_id,
    t1.status_code AS first_status,
    t1.location_code AS first_location,
    t1.event_timezone AS first_timezone,
    t1.event_time_local AS first_time_local,
    t2.status_code AS second_status,
    t2.location_code AS second_location,
    t2.event_timezone AS second_timezone,
    t2.event_time_local AS second_time_local,
    -- Calculate elapsed time accounting for time zones
    EXTRACT(EPOCH FROM (t2.event_time_local - t1.event_time_local))/3600 AS hours_between_local
FROM 
    amzlcore.v_package_journey_with_locations t1
JOIN 
    amzlcore.v_package_journey_with_locations t2
    ON t1.tracking_id = t2.tracking_id
    AND t1.event_time_utc < t2.event_time_utc
    AND t1.location_code != t2.location_code
    AND t1.event_timezone != t2.event_timezone
WHERE 
    -- Look for potential time anomalies (local time at destination earlier than origin)
    t2.event_time_local < t1.event_time_local
ORDER BY
    (t1.event_time_local - t2.event_time_local) DESC;

-- 8.2 Analyze planning effectiveness by time of day (local time)
SELECT
    cp.station_code,
    EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', map.timezone, cp.dcap_run_time_local)) AS planning_hour_local,
    COUNT(*) AS planned_packages,
    SUM(cp.is_inducted_as_planned) AS executed_as_planned,
    ROUND(SUM(cp.is_inducted_as_planned)::FLOAT / NULLIF(COUNT(*), 0) * 100, 1) AS execution_rate,
    AVG(EXTRACT(EPOCH FROM (cp.induct_datetime_local - cp.dcap_run_time_local))/60) AS avg_plan_to_induct_mins
FROM
    amzlcore.d_container_planning_events cp
JOIN
    amzl_mapping map ON cp.station_code = map.node
WHERE
    cp.dcap_run_time_local IS NOT NULL
GROUP BY
    cp.station_code, 
    EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', map.timezone, cp.dcap_run_time_local))
ORDER BY
    cp.station_code,
    planning_hour_local;