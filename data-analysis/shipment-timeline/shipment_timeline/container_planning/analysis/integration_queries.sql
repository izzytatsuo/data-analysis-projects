-- Container Planning Integration SQL Queries
-- These queries integrate container planning data with the shipment timeline structure

-- ---------------------------------------------------------
-- 1. Create container planning table in Redshift
-- ---------------------------------------------------------

CREATE TABLE amzlcore.d_container_planning_events (
    tracking_id VARCHAR(300) DISTKEY,
    shipment_id BIGINT,
    package_id INT,
    station_code VARCHAR(60),
    date DATE SORTKEY,
    container_plan_id VARCHAR(150),
    originating_node VARCHAR(65535),
    originating_fc_or_sc VARCHAR(6),
    is_planned INT,
    is_inducted INT,
    is_inducted_as_planned INT,
    is_planned_not_inducted INT,
    is_inducted_not_planned INT,
    dcap_run_time_local TIMESTAMP,
    induct_datetime_local TIMESTAMP,
    stow_datetime TIMESTAMP,
    slam_datetime_local TIMESTAMP,
    actual_ds_arrival_datetime_local TIMESTAMP,
    promised_arrival_datetime TIMESTAMP,
    condition VARCHAR(22),
    route_id VARCHAR(20),
    stop_number INT,
    bag_or_ov VARCHAR(8),
    induct_sort_zone VARCHAR(300),
    stow_sort_zone VARCHAR(144)
)
DISTSTYLE KEY
SORTKEY (date);

-- ---------------------------------------------------------
-- 2. Integrated view of container planning and shipment timeline
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_shipment_container_timeline AS
WITH timeline_base AS (
    -- Base shipment timeline from consolidated timeline table
    SELECT 
        tracking_id,
        shipment_key,
        status_code,
        status_description,
        status_time,
        source,
        status_level_tertiary,
        status_level_secondary,
        status_level_primary
    FROM 
        amzlcore.d_shipment_timeline
),
container_events AS (
    -- Container planning events transformed into timeline format
    SELECT
        cp.tracking_id,
        cp.shipment_id AS shipment_key,
        CASE
            WHEN event_type = 'PLAN' THEN 'CONTAINER_PLANNED'
            WHEN event_type = 'INDUCT' THEN 'CONTAINER_INDUCTED'
            WHEN event_type = 'STOW' THEN 'CONTAINER_STOWED'
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
        END AS status_time,
        'CONTAINER_PLANNING' AS source,
        'CONTAINER' AS status_level_tertiary,
        CASE
            WHEN event_type = 'PLAN' THEN 'PLANNING'
            WHEN event_type = 'INDUCT' THEN 'INDUCTION'
            WHEN event_type = 'STOW' THEN 'STOWING'
        END AS status_level_secondary,
        'DELIVERY_STATION' AS status_level_primary
    FROM 
        amzlcore.d_container_planning_events cp
    CROSS JOIN (
        VALUES ('PLAN'), ('INDUCT'), ('STOW')
    ) AS events(event_type)
    WHERE
        -- Only include events that actually happened
        (event_type = 'PLAN' AND cp.dcap_run_time_local IS NOT NULL) OR
        (event_type = 'INDUCT' AND cp.induct_datetime_local IS NOT NULL AND cp.is_inducted = 1) OR
        (event_type = 'STOW' AND cp.stow_datetime IS NOT NULL)
)

-- Combine base timeline with container events
SELECT * FROM timeline_base
UNION ALL
SELECT * FROM container_events
ORDER BY tracking_id, status_time;

-- ---------------------------------------------------------
-- 3. Planning effectiveness by station query
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_container_planning_effectiveness AS
SELECT
    station_code,
    date,
    COUNT(*) AS total_packages,
    SUM(is_planned) AS planned_packages,
    SUM(is_inducted) AS inducted_packages,
    SUM(is_inducted_as_planned) AS inducted_as_planned,
    SUM(is_planned_not_inducted) AS planned_not_inducted,
    SUM(is_inducted_not_planned) AS inducted_not_planned,
    SUM(is_planned)::FLOAT / COUNT(*) * 100 AS planning_rate,
    SUM(is_inducted_as_planned)::FLOAT / NULLIF(SUM(is_planned), 0) * 100 AS planning_execution_rate,
    SUM(is_inducted_not_planned)::FLOAT / SUM(is_inducted) * 100 AS unplanned_induction_rate,
    COUNT(DISTINCT originating_node) AS unique_originating_nodes
FROM
    amzlcore.d_container_planning_events
GROUP BY
    station_code, date
ORDER BY
    date DESC, station_code;

-- ---------------------------------------------------------
-- 4. Timing analysis query
-- ---------------------------------------------------------

CREATE OR REPLACE VIEW amzlcore.v_container_planning_timing AS
SELECT
    station_code,
    date,
    originating_node,
    originating_fc_or_sc,
    AVG(EXTRACT(EPOCH FROM (induct_datetime_local - dcap_run_time_local))/60) AS avg_plan_to_induct_mins,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (induct_datetime_local - dcap_run_time_local))/60) AS median_plan_to_induct_mins,
    AVG(EXTRACT(EPOCH FROM (stow_datetime - induct_datetime_local))/60) AS avg_induct_to_stow_mins,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (stow_datetime - induct_datetime_local))/60) AS median_induct_to_stow_mins,
    COUNT(*) AS package_count
FROM
    amzlcore.d_container_planning_events
WHERE
    dcap_run_time_local IS NOT NULL
    AND induct_datetime_local IS NOT NULL
    AND stow_datetime IS NOT NULL
GROUP BY
    station_code, date, originating_node, originating_fc_or_sc
HAVING
    COUNT(*) >= 5
ORDER BY
    date DESC, station_code, package_count DESC;

-- ---------------------------------------------------------
-- 5. Example analytics queries
-- ---------------------------------------------------------

-- 5.1 Find packages with timeline gaps
SELECT 
    t1.tracking_id,
    t1.status_code AS first_status,
    t1.status_time AS first_time,
    t2.status_code AS second_status,
    t2.status_time AS second_time,
    EXTRACT(EPOCH FROM (t2.status_time - t1.status_time))/3600 AS hours_between
FROM 
    amzlcore.v_shipment_container_timeline t1
JOIN 
    amzlcore.v_shipment_container_timeline t2
    ON t1.tracking_id = t2.tracking_id
    AND t1.status_time < t2.status_time
    AND NOT EXISTS (
        SELECT 1 FROM amzlcore.v_shipment_container_timeline t3
        WHERE t3.tracking_id = t1.tracking_id
        AND t3.status_time > t1.status_time
        AND t3.status_time < t2.status_time
    )
WHERE 
    EXTRACT(EPOCH FROM (t2.status_time - t1.status_time))/3600 > 6  -- Gaps longer than 6 hours
ORDER BY
    hours_between DESC;

-- 5.2 Compare promised delivery dates with actuals
SELECT
    cp.tracking_id,
    cp.station_code,
    cp.promised_arrival_datetime,
    cp.actual_ds_arrival_datetime_local,
    EXTRACT(EPOCH FROM (cp.actual_ds_arrival_datetime_local - cp.promised_arrival_datetime))/3600 AS hours_diff,
    CASE
        WHEN cp.actual_ds_arrival_datetime_local <= cp.promised_arrival_datetime THEN 'On Time'
        ELSE 'Late'
    END AS delivery_status,
    cp.condition,
    cp.route_id,
    cp.originating_node,
    cp.is_inducted_as_planned
FROM
    amzlcore.d_container_planning_events cp
WHERE
    cp.promised_arrival_datetime IS NOT NULL
    AND cp.actual_ds_arrival_datetime_local IS NOT NULL;

-- 5.3 Analyze relationship between planning compliance and delivery performance
SELECT
    station_code,
    CASE
        WHEN is_inducted_as_planned = 1 THEN 'Inducted as Planned'
        WHEN is_inducted_not_planned = 1 THEN 'Inducted Not Planned'
        WHEN is_planned_not_inducted = 1 THEN 'Planned Not Inducted'
        ELSE 'Other'
    END AS planning_status,
    COUNT(*) AS package_count,
    AVG(CASE WHEN actual_ds_arrival_datetime_local <= promised_arrival_datetime 
             THEN 1 ELSE 0 END) * 100 AS on_time_percentage,
    AVG(EXTRACT(EPOCH FROM (actual_ds_arrival_datetime_local - promised_arrival_datetime))/3600) AS avg_arrival_diff_hours
FROM
    amzlcore.d_container_planning_events
WHERE
    promised_arrival_datetime IS NOT NULL
    AND actual_ds_arrival_datetime_local IS NOT NULL
GROUP BY
    station_code, planning_status
ORDER BY
    station_code, planning_status;

-- ---------------------------------------------------------
-- 6. Sample data loading (for reference)
-- ---------------------------------------------------------

-- To load sample data from CSV:
COPY amzlcore.d_container_planning_events 
FROM 's3://altdatasetexfil/claudecloud/container_planning_sample.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftLoadRole'
FORMAT AS CSV
IGNOREHEADER 1;

-- To load from parquet files directly:
COPY amzlcore.d_container_planning_events 
FROM 's3://altdatasetexfil/claudecloud/routing2_container_snip/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftLoadRole'
FORMAT AS PARQUET;