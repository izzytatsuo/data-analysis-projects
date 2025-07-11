/*
Shipment Timeline Query
----------------------
This query creates a consolidated timeline of shipment events from multiple sources:
- slam_packages_leg_live (shipment legs)
- gmp_shipment_events_na (shipment tracking events)
- package_systems_event_na (package status events)

The timeline shows the chronological progression of events for each shipment/package.
*/

-- Step 1: Create a temp table for node mapping (locations)
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

-- Step 2: Create a temp view for load summary data (to join with later)
DROP TABLE IF EXISTS tv_load_summary;
CREATE TEMP TABLE tv_load_summary DISTKEY(vrid) SORTKEY(final_destination) AS (
    SELECT
        vrid,
        origin,
        final_destination,
        origin_type,
        destination_type,
        lane,
        cpt,
        miles,
        origin_local_timezone,
        origin_scheduled_arrival,
        origin_calc_arrival,
        origin_arrival_late_group,
        origin_arrival_late_hrs,
        origin_arrival_reason,
        origin_scheduled_depart,
        origin_calc_depart,
        origin_departure_late_group,
        origin_depart_late_hrs,
        dest_local_timezone,
        dest_scheduled_arrival,
        dest_calc_arrival,
        dest_arrival_late_group,
        dest_arrival_late_hrs,
        dest_arrival_reason,
        account_id,
        report_day,
        vr_create_date,
        'VLS' AS source_system
    FROM
        "andes"."ats-onestopshop"."v_load_summary_hourly"
    WHERE
        1=1
        AND region_id = 'NA'
        AND dest_country IN ('US', 'CA')
        AND report_day > GETDATE() - INTERVAL '3 DAYS' -- Filter to last 3 days
);

-- Step 3: Create a temp table for slam leg data (shipment legs)
DROP TABLE IF EXISTS slam_legs;
CREATE TEMP TABLE slam_legs DISTKEY(shipment_id) SORTKEY(pickup_date) AS (
    WITH leg_data AS (
        SELECT
            leg.leg_warehouse_id,
            leg.leg_destination_warehouse_id,
            leg.shipment_id,
            leg.package_id,
            leg.pickup_date,
            leg.estimated_arrival_date,
            leg.leg_ship_method,
            leg.request_timestamp,
            leg.leg_sequence_id,
            ROW_NUMBER() OVER (
                PARTITION BY leg.shipment_id, leg.package_id
                ORDER BY leg.request_timestamp DESC, leg.pickup_date DESC
            ) AS leg_rn
        FROM
            "backlog_datasets"."atrops"."o_slam_packages_leg_live" leg
        WHERE
            leg.request_timestamp > GETDATE() - INTERVAL '3 DAYS' -- Filter to last 3 days
    )
    SELECT
        leg_warehouse_id AS location_id,
        leg_destination_warehouse_id AS destination_id,
        shipment_id,
        package_id,
        pickup_date,
        estimated_arrival_date,
        leg_ship_method,
        request_timestamp,
        leg_sequence_id,
        'SLAM_LEG_PICKUP' AS event_type,
        pickup_date AS event_timestamp,
        'SLAM_LEG' AS source_system,
        NULL AS tracking_id,
        NULL AS status_code
    FROM
        leg_data
    WHERE
        leg_rn = 1 -- Most recent leg for each shipment/package
);

-- Step 4: Create a temp table for gmp events data
DROP TABLE IF EXISTS gmp_events;
CREATE TEMP TABLE gmp_events DISTKEY(shipment_id) SORTKEY(status_date) AS (
    WITH gmp_data AS (
        SELECT
            gmp.shipment_type,
            gmp.sender_id,
            gmp.tracking_id,
            gmp.ship_track_event_code,
            gmp.standard_carrier_alpha_code,
            gmp.status_node_id,
            gmp.status_date,
            gmp.status_code,
            gmp.fulfillment_shipment_id AS shipment_id,
            gmp.package_id,
            gmp.parent_container_id AS vrid,
            ROW_NUMBER() OVER (
                PARTITION BY gmp.tracking_id 
                ORDER BY gmp.status_date DESC
            ) AS status_rn
        FROM
            "backlog_datasets"."amzlcore"."gmp_shipment_events_na" gmp
        INNER JOIN
            slam_legs sl ON gmp.fulfillment_shipment_id = sl.shipment_id
        WHERE
            gmp.dw_created_time > GETDATE() - INTERVAL '3 DAYS' -- Filter to last 3 days
    )
    SELECT
        status_node_id AS location_id,
        NULL AS destination_id,
        shipment_id,
        package_id,
        NULL AS pickup_date,
        NULL AS estimated_arrival_date,
        NULL AS leg_ship_method,
        status_date AS request_timestamp,
        NULL AS leg_sequence_id,
        ship_track_event_code AS event_type,
        status_date AS event_timestamp,
        'GMP' AS source_system,
        tracking_id,
        status_code,
        vrid
    FROM
        gmp_data
);

-- Step 5: Create a tracking ID lookup table to link GMP and PSE
DROP TABLE IF EXISTS tracking_id_lookup;
CREATE TEMP TABLE tracking_id_lookup DISTKEY(tracking_id) SORTKEY(shipment_id) AS (
    SELECT DISTINCT
        shipment_id,
        package_id,
        tracking_id
    FROM
        gmp_events
    WHERE
        tracking_id IS NOT NULL
);

-- Step 6: Create a temp table for package system events data
DROP TABLE IF EXISTS pse_events;
CREATE TEMP TABLE pse_events DISTKEY(tracking_id) SORTKEY(event_timestamp) AS (
    SELECT
        pse.state_location_id AS location_id,
        pse.state_location_destination_id AS destination_id,
        til.shipment_id,
        til.package_id,
        NULL AS pickup_date,
        NULL AS estimated_arrival_date,
        NULL AS leg_ship_method,
        pse.state_time AS request_timestamp,
        NULL AS leg_sequence_id,
        pse.state_status AS event_type,
        pse.state_time AS event_timestamp,
        'PSE' AS source_system,
        pse.forward_tracking_id AS tracking_id,
        pse.state_sub_status AS status_code
    FROM
        "backlog_datasets"."amzlcore"."package_systems_event_na" pse
    INNER JOIN
        tracking_id_lookup til ON pse.forward_tracking_id = til.tracking_id
    WHERE
        pse.dw_created_time > GETDATE() - INTERVAL '3 DAYS' -- Filter to last 3 days
);

-- Step 7: Create the final consolidated timeline table
DROP TABLE IF EXISTS shipment_timeline;
CREATE TABLE shipment_timeline DISTKEY(shipment_id) SORTKEY(event_timestamp) AS (
    -- SLAM Leg events (pickup events)
    SELECT
        shipment_id,
        package_id,
        tracking_id,
        event_timestamp,
        event_type,
        status_code,
        location_id,
        destination_id,
        source_system,
        NULL AS vrid,
        estimated_arrival_date
    FROM
        slam_legs
    
    UNION ALL
    
    -- SLAM Leg events (estimated arrival events)
    SELECT
        shipment_id,
        package_id,
        tracking_id,
        estimated_arrival_date AS event_timestamp,
        'SLAM_LEG_ARRIVAL' AS event_type,
        NULL AS status_code,
        destination_id AS location_id,
        NULL AS destination_id,
        'SLAM_LEG' AS source_system,
        NULL AS vrid,
        NULL AS estimated_arrival_date
    FROM
        slam_legs
    WHERE
        estimated_arrival_date IS NOT NULL
    
    UNION ALL
    
    -- GMP events
    SELECT
        shipment_id,
        package_id,
        tracking_id,
        event_timestamp,
        event_type,
        status_code,
        location_id,
        destination_id,
        source_system,
        vrid,
        NULL AS estimated_arrival_date
    FROM
        gmp_events
    
    UNION ALL
    
    -- PSE events
    SELECT
        shipment_id,
        package_id,
        tracking_id,
        event_timestamp,
        event_type,
        status_code,
        location_id,
        destination_id,
        source_system,
        NULL AS vrid,
        NULL AS estimated_arrival_date
    FROM
        pse_events
);

-- Step 8: Add location information to the timeline
DROP TABLE IF EXISTS shipment_timeline_enriched;
CREATE TABLE shipment_timeline_enriched DISTKEY(shipment_id) SORTKEY(event_timestamp) AS (
    SELECT
        st.*,
        origin_map.region AS origin_region,
        origin_map.country AS origin_country,
        origin_map.timezone AS origin_timezone,
        dest_map.region AS destination_region,
        dest_map.country AS destination_country,
        dest_map.timezone AS destination_timezone,
        vls.lane,
        vls.miles,
        vls.origin_scheduled_arrival,
        vls.origin_calc_arrival,
        vls.origin_arrival_late_hrs,
        vls.origin_scheduled_depart,
        vls.origin_calc_depart,
        vls.origin_depart_late_hrs,
        vls.dest_scheduled_arrival,
        vls.dest_calc_arrival,
        vls.dest_arrival_late_hrs,
        DATEDIFF('minute', LAG(st.event_timestamp) OVER (
            PARTITION BY st.shipment_id, st.package_id
            ORDER BY st.event_timestamp
        ), st.event_timestamp) / 60.0 AS hours_since_previous_event,
        ROW_NUMBER() OVER (
            PARTITION BY st.shipment_id, st.package_id
            ORDER BY st.event_timestamp
        ) AS event_sequence
    FROM
        shipment_timeline st
    LEFT JOIN
        amzl_mapping origin_map ON st.location_id = origin_map.node
    LEFT JOIN
        amzl_mapping dest_map ON st.destination_id = dest_map.node
    LEFT JOIN
        tv_load_summary vls ON st.vrid = vls.vrid
);

-- Create a final analysis view to make querying easier
DROP TABLE IF EXISTS shipment_journey_analysis;
CREATE TABLE shipment_journey_analysis AS (
    SELECT
        shipment_id,
        package_id,
        tracking_id,
        COUNT(DISTINCT source_system) AS num_systems_involved,
        MIN(event_timestamp) AS first_event_time,
        MAX(event_timestamp) AS last_event_time,
        DATEDIFF('minute', MIN(event_timestamp), MAX(event_timestamp)) / 60.0 AS total_journey_hours,
        COUNT(*) AS total_events,
        SUM(CASE WHEN source_system = 'SLAM_LEG' THEN 1 ELSE 0 END) AS slam_events,
        SUM(CASE WHEN source_system = 'GMP' THEN 1 ELSE 0 END) AS gmp_events,
        SUM(CASE WHEN source_system = 'PSE' THEN 1 ELSE 0 END) AS pse_events,
        LISTAGG(event_type, ' → ') WITHIN GROUP (ORDER BY event_timestamp) AS event_sequence
    FROM
        shipment_timeline_enriched
    GROUP BY
        shipment_id, package_id, tracking_id
);

-- Example query to retrieve timeline for a specific shipment
-- Replace {shipment_id} with an actual shipment ID to view its timeline
/*
SELECT
    event_sequence,
    event_timestamp,
    event_type,
    source_system,
    location_id,
    destination_id,
    status_code,
    hours_since_previous_event
FROM
    shipment_timeline_enriched
WHERE
    shipment_id = '{shipment_id}'
ORDER BY
    event_timestamp;
*/