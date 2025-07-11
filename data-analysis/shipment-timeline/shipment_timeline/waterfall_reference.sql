/* RQEV2-CYNY0Xrfb6 */

CREATE TABLE waterfall_v6 SORTKEY (pk, event_timestamp) DISTKEY (pk) AS (

SELECT
    ST.source
  , ST.pk
  , ST.shipment_id
  , ST.package_id
  , ST.tracking_id
  /* timezone conversions */
  , ST.event_timestamp
  , ST.request_timestamp
  , ST.event_timestamp::TIMESTAMPTZ AS event_timestamptz
  , ST.request_timestamp::TIMESTAMPTZ AS request_timestamptz
  -- this is obviously not optimal, three window funcs, but just keeping for now
  , LAST_VALUE(ST."zone") IGNORE NULLS OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_slammed_zone
  , LAST_VALUE(ST.delivery_group_pickup_date) IGNORE NULLS OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_slammed_cpt
  , LAST_VALUE(ST.ship_method) IGNORE NULLS OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_ship_method
--   , LAST_VALUE(ST."zone" || '#' || ST.delivery_group_pickup_date || '#' || ST.ship_method) IGNORE NULLS OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_slammed_zone


-- for future_versions, we may just be better off computing latest slammed zone later?? so that we don't nest window functions and can get status path???




--------------------------------------------------------------------------------
/*                               TERTIARY STATUS                              */
--------------------------------------------------------------------------------
  /* Single classification case statement for all event types */
  , CASE 
      -- Transit and middle mile scans
      WHEN ST.ship_track_event_code = 'EVENT_254' THEN
        CASE
          WHEN ST.status_node_id = latest_slammed_zone                                  THEN 'UNCLASSIFIED' -- this is actually CHECKOUT_FROM_DELIVERY_NODE after arrival. There is also the chance that the truck arrives at the ds, then the package is reslammed, the truck leaves and thus the package is not caught in this condition. Calling it 'UNCLASSIFIED' for use in QUALIFY removal condition. See status path video for more deets
          WHEN ST.final_destination = latest_slammed_zone
                                                                                        THEN 'IN_TRANSIT_TO_DELIVERY_NODE'
                                                                                        ELSE 'IN_TRANSIT_TO_MIDDLE_MILE_NODE'
        END
      WHEN ST.ship_track_event_code = 'EVENT_253' THEN
        CASE
          WHEN ST.status_node_id = latest_slammed_zone
                                                                                        THEN 'ARRIVED_AT_DELIVERY_NODE'
                                                                                        ELSE 'ARRIVED_AT_MIDDLE_MILE_NODE'
        END
      WHEN ST.ship_track_event_code = 'EVENT_201'
                                    /* AND supplement_code = 'AMAZON_FACILITY' */       THEN 'SCANNED_AT_MIDDLE_MILE_NODE'
      
      -- received, induct, other events
      WHEN ST.state_status = 'RECEIVED' AND state_sub_status <> 'SELF_SERVICE_RETURN'   THEN 'RECEIVED'
      WHEN ST.state_status = 'INDUCTED'                                                 THEN 'INDUCTED'
      WHEN ST.state_status = 'HELD' THEN
        CASE
          WHEN ST.state_sub_status IN ('COMMERCIAL', 'FDD')                             THEN 'SIDELINED'
                                                                                        ELSE 'HELD'
        END
      -- Processing events
      WHEN ST.state_status IN ('STOWED', 'STAGE_BUFFERED')                              THEN 'STOWED' -- stage buffered can occur direcly after marked_for_reprocess
      WHEN ST.state_status = 'STOW_BUFFERED'                                            THEN 'INDUCTED'
      WHEN ST.state_status IN ('PICKED', 'PICKED_FROM_BUFFER')                          THEN 'PICKED'
      WHEN ST.state_status = 'STAGED'                                                   THEN 'STAGED'
      
      -- Dispatch events
      WHEN ST.state_status = 'IN_TRANSIT' AND ST.state_location_destination_type IN('CUSTOMER_ADDRESS', 'LAST_MILE_ACCESS_POINT')
                                                                                        THEN 'OUT_FOR_DELIVERY'
      WHEN ST.ship_track_event_code = 'EVENT_302'                                       THEN 'OUT_FOR_DELIVERY'


      WHEN ST.ship_track_event_code = 'EVENT_304'                                       THEN 'ATTEMPTED'
      WHEN ST.ship_track_event_code = 'EVENT_301'                                       THEN 'DELIVERED'
      
      WHEN ST.ship_track_event_code = 'EVENT_258'                                       THEN 'EXTENDED_DELAY_DELIVERABLE'


      WHEN ST.state_status = 'IN_TRANSIT' AND state_location_destination_type = 'LAST_MILE_NODE' AND state_location_source_type = 'LAST_MILE_NODE' THEN
        CASE
          WHEN state_sub_status = 'CARRIER_SWITCHED'                                    THEN 'IN_TRANSIT_BETWEEN_DS_CARRIER_SWITCH'
                                                                                        ELSE 'IN_TRANSIT_BETWEEN_DS'
        END

      WHEN ST.state_status = 'MARKED_AS_MISSING'                                        THEN 'MARKED_AS_MISSING' -- likely exclusion, potential for delivery later
      -- Terminal/conclusion events
      WHEN ((ST.state_status = 'IN_TRANSIT' AND ST.state_location_destination_type = 'MIDDLE_MILE_NODE') -- this may remove some packages it shouldn't, maybe swa goes back to sc then to ds again???
            OR
            (ST.state_status = 'DISPOSED')
            OR
            (ST.state_status = 'PICKUP_FAILED' AND state_sub_status = 'TR_CANCELLED')
            OR
            (ST.state_status = 'MARKED_AS_LOST')
            OR
            (ST.state_status = 'MARKED_FOR_REPROCESS' AND (state_sub_status LIKE '%FC%' OR state_sub_status = 'CUSTOMER_CANCELLATION'))
            OR
            (ST.state_status = 'MARKED_FOR_PROBLEM' AND state_sub_status = 'DAMAGED')
            OR
            (ST.state_status = 'RECEIVED' AND state_sub_status = 'SELF_SERVICE_RETURN')
            OR
            ST.ship_track_event_code IN('EVENT_250'
                                        'EVENT_760'
                                        'EVENT_762'
                                        'EVENT_764'
                                        'EVENT_408'
                                        'EVENT_476')
            )
                                                                                        THEN 'UNDELIVERABLE'

      WHEN ST.ship_track_event_code = 'EVENT_108'                                       THEN 'SWA_PICKUP_FAILURE'
      WHEN ST.ship_track_event_code = 'EVENT_407'                                       THEN 'SWA_REFUSAL'
      -- Delivery events
      WHEN ST.state_status IN('DELIVERED', 'DROPPED')                                   THEN 'DELIVERED'
      -- Default for unclassified events
      WHEN partition_source = 'SLAM'                                                    THEN 'SLAMMED'
      -- likely removing pse slam
      WHEN ST.state_status = 'MANIFESTED'                                               THEN 'SLAMMED'
      ELSE COALESCE(ST.state_status, 'UNCLASSIFIED')
    END AS tertiary_status

--------------------------------------------------------------------------------
/*                              SECONDARY STATUS                              */
--------------------------------------------------------------------------------
, CASE 
    WHEN tertiary_status = 'SLAMMED'                                                    THEN 'SLAMMED'
    WHEN tertiary_status IN (
        'SCANNED_AT_MIDDLE_MILE_NODE',
        'IN_TRANSIT_TO_MIDDLE_MILE_NODE', 
        'ARRIVED_AT_MIDDLE_MILE_NODE'
    ) THEN 'IN_TRANSIT_MIDDLE_MILE'
    WHEN tertiary_status = 'IN_TRANSIT_TO_DELIVERY_NODE'                                THEN 'IN_TRANSIT_TO_DS'
    WHEN tertiary_status = 'ARRIVED_AT_DELIVERY_NODE'                                   THEN 'IN_YARD'
    WHEN tertiary_status = 'RECEIVED'                                                   THEN 'RECEIVED'
    WHEN tertiary_status = 'INDUCTED'                                                   THEN 'INDUCTED'
    WHEN tertiary_status = 'SIDELINED'                                                  THEN 'SIDELINED'
    WHEN tertiary_status = 'HELD'                                                       THEN 'HELD'
    WHEN tertiary_status = 'STOWED'                                                     THEN 'STOWED'
    WHEN tertiary_status = 'PICKED'                                                     THEN 'PICKED'
    WHEN tertiary_status = 'STAGED'                                                     THEN 'STAGED'
    WHEN tertiary_status = 'MARKED_AS_MISSING'                                          THEN 'MARKED_AS_MISSING'
    WHEN tertiary_status = 'UNDELIVERABLE'                                              THEN 'UNDELIVERABLE'

    -- if using gmp attempted logic, will need to default to pse for returns data
    -- potential to check for pse event between dispatch. Or maybe alter the logic
    -- dependent on what is required. Only use attempted if pse is missing for
    -- some reason. 
    WHEN tertiary_status IN (
        'ATTEMPTED',
        'EXTENDED_DELAY_DELIVERABLE', -- this may no actuall be in station
        'DELIVERY_REJECTED',
        'MARKED_FOR_PROBLEM',
        'DELIVERY_FAILED',
        'MARKED_FOR_REPROCESS'
    )                                                                                   THEN 'IN_STATION_DELIVERABLE'
    WHEN tertiary_status IN (
        'SWA_PICKUP_FAILURE',
        'SWA_REFUSAL'
    )                                                                                   THEN 'EXCLUDED_DELIVERABLE_SWA'
    WHEN tertiary_status IN (
        'IN_TRANSIT_BETWEEN_DS',
        'IN_TRANSIT_BETWEEN_DS_CARRIER_SWITCH'
    )                                                                                   THEN 'MISC_EXCLUSION'
    WHEN tertiary_status = 'OUT_FOR_DELIVERY'                                           THEN 'OUT_FOR_DELIVERY'
    WHEN tertiary_status = 'DELIVERED'                                                  THEN 'DELIVERED'
                                                                                        ELSE 'OTHER'
END AS secondary_status





--------------------------------------------------------------------------------
/*                               PRIMARY STATUS                               */
--------------------------------------------------------------------------------
, CASE 
    WHEN secondary_status IN (
        'SLAMMED',
        'IN_TRANSIT_MIDDLE_MILE',
        'IN_TRANSIT_TO_DS'
    ) THEN 'UPSTREAM'
    
    WHEN secondary_status IN (
        'IN_YARD',
        'RECEIVED',
        'INDUCTED',
        'SIDELINED',
        'HELD',
        'STOWED',
        'PICKED',
        'STAGED',
        'IN_STATION_DELIVERABLE',
        'MARKED_AS_MISSING'
    ) THEN 'IN_STATION'
    
    WHEN secondary_status = 'OUT_FOR_DELIVERY' THEN 'OUT_FOR_DELIVERY'
    
    WHEN secondary_status = 'DELIVERED' THEN 'DELIVERED'
    
    WHEN secondary_status IN (
        'UNDELIVERABLE',
        'EXCLUDED_DELIVERABLE_SWA',
        'MISC_EXCLUSION'
    ) THEN 'UNDELIVERABLE'
    
    ELSE 'OTHER'
END AS primary_status


, MAX(
CASE WHEN (ST.state_status IN('DELIVERED', 'DROPPED') or ST.ship_track_event_code = 'EVENT_301') THEN 1 ELSE 0 END) IGNORE NULLS OVER(
PARTITION by ST.pk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS was_delivered


-- , ROW_NUMBER() OVER(PARTITION BY ST.pk ORDER BY ST.event_timestamptz DESC) as rn



/* SLAM */
, ST.warehouse_id
, ST.pickup_date
, ST.ship_option
, ST.carrier_name
, ST.estimated_arrival_date
, ST.external_estimated_arrival_date
, ST.delivery_group_pickup_date
, ST.ship_method
/* PSE */
, ST.state_status
, ST.state_sub_status
, ST.state_location_destination_type
, ST.state_location_source_type
, ST.reverse_tracking_id
/* GMP */
, load_id
, supplement_code
, ship_track_event_code
-- , status_code
, ST.status_node_id
, STATUS_NODE_MAPPING.location_type AS status_node_mapping
, ST.origin
, ST.origin_type
, ORIGIN_MAPPING.location_type AS origin_mapping
, ST.final_destination as destination
, ST.destination_type as destination_type
, DESTINATION_MAPPING.location_type AS destination_mapping

, COALESCE(dw_created_time, leg_request_timestamp)::TIMESTAMPTZ AS dw_created_time -- unsure why slam event is missing from this

FROM
    dev.public.shipment_timeline_share ST
-- this join needs to be done in the earliest stage to v load summary
LEFT JOIN node_type ORIGIN_MAPPING ON ST.origin = ORIGIN_MAPPING.location_id
LEFT JOIN node_type STATUS_NODE_MAPPING ON ST.status_node_id = STATUS_NODE_MAPPING.location_id
LEFT JOIN node_type DESTINATION_MAPPING ON ST.final_destination = DESTINATION_MAPPING.location_id

WHERE
    1=1
    AND partition_source IN('SLAM', 'PSE')
    OR ST.ship_track_event_code IN (
    'EVENT_108',    -- SWA Pickup Failure
    'EVENT_250',    -- Sender cancelled pick up (MFN?)
    'EVENT_253',    -- ATS Arrival
    'EVENT_254',    -- ATS Departure
    'EVENT_258',    -- Identify shipments that can be delivered beyond FRD and publish a new EDD to Customers
    'EVENT_301',    -- Delivery
    'EVENT_302',    -- Dispatch
    'EVENT_304',    -- Attempt
    'EVENT_407',    -- Recipient refused (MFN?)
    'EVENT_408',    -- Shipment is undeliverable and is returning to Amazon or to MFN seller
    'EVENT_476',    -- Potential lost
    'EVENT_760',    -- Shipment damaged and will be replaced
    'EVENT_762',    -- Missorted and will be returned
    'EVENT_764'     -- Mislabled and will be returned
    -- 'EVENT_201'                -- updating this for middle mile visibility. Woot (dropship?) only available at slam. Delivery depot check is redundant, makes code more difficult
    -- 'EVENT_202',    -- Departure -- removing due to no final_destination or vrid join available
    -- 'EVENT_208',    -- SWA, need to classify (5/25/25)
    -- 'EVENT_503'     -- Secured pick up appointment date || AGL amazon global listing, spot check occured at slam || https://w.amazon.com/bin/view/TxNotifications/upstream/AGL
    -- EVENT_102, 'EVENT_103', 'EVENT_104', 'EVENT_216', 'EVENT_228', 'EVENT_238', 'EVENT_307', 'EVENT_308', 'EVENT_310', 'EVENT_320', 'EVENT_402', 'EVENT_404', 'EVENT_660', 'EVENT_661'
    )
    OR (ST.ship_track_event_code = 'EVENT_201' AND supplement_code = 'AMAZON_FACILITY') -- updating this for middle mile visibility. Woot, presumably dropship will show the shipment's arrival at fc (presumably confirming it was received from seller. So a confirmation after slam)
QUALIFY
  tertiary_status <> 'UNCLASSIFIED'
ORDER BY
    pk,
    event_timestamp
)