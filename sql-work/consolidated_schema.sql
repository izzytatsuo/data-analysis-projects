/*
Consolidated Schema Documentation
--------------------------------

This file contains consolidated column definitions for supply chain tracking systems,
with a focus on v_load_summary_hourly and related tracking tables.
*/

-- v_load_summary_hourly: Contains vehicle routing data
CREATE TABLE v_load_summary_hourly (
  -- Identifiers
  vrid VARCHAR,                         -- Unique vehicle route ID
  tr_id VARCHAR,                        -- Transport record ID
  tp_id VARCHAR,                        -- Transport plan ID
  tour_id VARCHAR,                      -- Tour identifier
  crid VARCHAR,                         -- Customer reference ID
  account_id VARCHAR,                   -- Business purpose identifier
  
  -- Region and reporting
  region_id VARCHAR,                    -- Region identifier (e.g., NA)
  report_day TIMESTAMP,                 -- Report day timestamp
  report_week VARCHAR,                  -- Week of the report (e.g., W27)
  report_month VARCHAR,                 -- Month of the report (e.g., M07)
  
  -- Carrier information
  scac VARCHAR,                         -- Standard Carrier Alpha Code
  carrier_name VARCHAR,                 -- Name of the carrier
  subcarrier VARCHAR,                   -- Subcarrier code
  carrier_group VARCHAR,                -- Carrier group (e.g., ATS_BROKERAGE)
  carrier_manager VARCHAR,              -- Carrier manager
  
  -- Route details
  lane VARCHAR,                         -- Origin to destination route (e.g., DCL4->CLE2)
  stop_count INT,                       -- Number of stops
  miles INT,                            -- Distance in miles
  program_code VARCHAR,                 -- Purpose code (EMPTY, XFER, SCIB, AMZL, etc.)
  shipment_mode VARCHAR,                -- Mode (TRUCKLOAD, INTERMODAL, LESS_THAN_TRUCKLOAD)
  equipment_type VARCHAR,               -- Type of equipment/vehicle
  cpt TIMESTAMP,                        -- Critical pull time/commitment
  adhoc_load VARCHAR,                   -- Whether the load is adhoc (TRUE/FALSE)
  canceled_load VARCHAR,                -- Whether load was canceled (TRUE/FALSE)
  canceled_date TIMESTAMP,              -- Date and time of cancellation
  cancelation_reason VARCHAR,           -- Reason for cancellation
  vehicle_execution_status VARCHAR,     -- Status (COMPLETED, CANCELLED, etc.)
  
  -- Origin details
  origin VARCHAR,                       -- Origin facility code
  origin_zip VARCHAR,                   -- Origin ZIP code
  origin_city VARCHAR,                  -- Origin city
  origin_state VARCHAR,                 -- Origin state
  origin_country VARCHAR,               -- Origin country
  origin_type VARCHAR,                  -- Type of origin facility
  origin_local_timezone VARCHAR,        -- Local timezone of origin
  
  -- Destination details
  final_destination VARCHAR,            -- Destination facility code
  dest_zip VARCHAR,                     -- Destination ZIP code
  dest_city VARCHAR,                    -- Destination city
  dest_state VARCHAR,                   -- Destination state
  dest_country VARCHAR,                 -- Destination country
  destination_type VARCHAR,             -- Type of destination facility
  dest_local_timezone VARCHAR,          -- Local timezone of destination
  
  -- Timing information - Origin
  origin_scheduled_arrival TIMESTAMP,   -- Scheduled arrival at origin
  origin_calc_arrival TIMESTAMP,        -- Calculated/actual arrival at origin
  origin_calc_arrival_source VARCHAR,   -- Source of origin arrival calculation
  origin_arrival_late_group VARCHAR,    -- Grouping for origin arrival lateness
  origin_arrival_late_hrs DECIMAL,      -- Hours late at origin arrival (negative is early)
  origin_arrival_reason VARCHAR,        -- Reason for arrival timing at origin
  origin_begin_loading_time TIMESTAMP,  -- Beginning of loading at origin
  origin_finish_loading_time TIMESTAMP, -- End of loading at origin
  origin_scheduled_depart TIMESTAMP,    -- Scheduled departure from origin
  origin_calc_depart TIMESTAMP,         -- Calculated/actual departure from origin
  origin_calc_depart_source VARCHAR,    -- Source of origin departure calculation
  origin_departure_late_group VARCHAR,  -- Grouping for origin departure lateness
  origin_depart_late_hrs DECIMAL,       -- Hours late at origin departure (negative is early)
  origin_fc_delay_hours DECIMAL,        -- Hours of delay at origin facility
  
  -- Timing information - Destination
  dest_scheduled_arrival TIMESTAMP,     -- Scheduled arrival at destination
  dest_calc_arrival TIMESTAMP,          -- Calculated/actual arrival at destination
  dest_calc_arrival_source VARCHAR,     -- Source of destination arrival calculation
  dest_arrival_late_group VARCHAR,      -- Grouping for destination arrival lateness
  dest_arrival_late_hrs DECIMAL,        -- Hours late at destination arrival (negative is early)
  dest_arrival_reason VARCHAR,          -- Reason for arrival timing at destination
  dest_begin_unloading_time TIMESTAMP,  -- Beginning of unloading at destination
  dest_finish_unloading_time TIMESTAMP, -- End of unloading at destination
  
  -- Transit information
  transit_hours_actual DECIMAL,         -- Actual transit hours
  scheduled_transit_hours DECIMAL,      -- Scheduled transit hours
  
  -- Cost information
  manifest_base DECIMAL,                -- Base manifest cost
  manifest_fuel DECIMAL,                -- Fuel manifest cost
  manifest_total DECIMAL,               -- Total manifest cost
  total_invoice_amount DECIMAL,         -- Total invoice amount
  total_paid_amount DECIMAL,            -- Total paid amount
  total_accessorials DECIMAL,           -- Total accessorial charges
  estimated_cost_accrual DECIMAL,       -- Estimated cost accrual
  accrual_cost_source VARCHAR,          -- Source of accrual cost
  
  -- Equipment and personnel
  trailer_id VARCHAR,                   -- Trailer identifier
  driver_id VARCHAR,                    -- Driver identifier
  driver_id_2 VARCHAR,                  -- Secondary driver identifier
  power_id VARCHAR,                     -- Power unit identifier
  
  -- Creation and meta information
  vr_create_date TIMESTAMP              -- Vehicle route creation date
);

-- GMP Shipment Events Table: Tracking events for shipments
CREATE TABLE gmp_shipment_events_na (
  -- Key identifiers
  shipment_type VARCHAR,                -- Type of shipment
  tracking_id VARCHAR,                  -- Primary tracking identifier
  fulfillment_shipment_id VARCHAR,      -- Shipment ID (links to slam tables)
  package_id VARCHAR,                   -- Package identifier
  parent_container_id VARCHAR,          -- Parent container ID (can link to vrid)
  
  -- Event information
  ship_track_event_code VARCHAR,        -- Type of tracking event
  status_code VARCHAR,                  -- Current status of the shipment
  status_date TIMESTAMP,                -- When the status was recorded
  status_node_id VARCHAR,               -- Location where status was recorded
  
  -- Additional timestamps
  estimated_arrival_date TIMESTAMP,     -- Estimated arrival date
  pick_up_date TIMESTAMP,               -- When package was picked up
  actual_delivery_date TIMESTAMP,       -- Actual delivery timestamp
  
  -- Carrier information  
  standard_carrier_alpha_code VARCHAR,  -- Standard carrier code
  sub_carrier VARCHAR,                  -- Subcarrier identifier
  
  -- Creation metadata
  dw_created_time TIMESTAMP             -- Data warehouse creation timestamp
);

-- SLAM Packages Leg Table: Information about shipment legs
CREATE TABLE o_slam_packages_leg_live (
  -- Key identifiers  
  slam_leg_pk VARCHAR,                  -- Primary key for the leg record
  shipment_id INTEGER,                  -- ID of the shipment
  package_id INTEGER,                   -- ID of the package
  
  -- Leg information
  leg_sequence_id DECIMAL,              -- Sequence of the leg within route
  leg_warehouse_id VARCHAR,             -- Origin warehouse for this leg
  leg_destination_warehouse_id VARCHAR, -- Destination warehouse for this leg
  leg_ship_method VARCHAR,              -- Shipping method for this leg
  
  -- Timing information
  pickup_date TIMESTAMP,                -- Pickup date for this leg
  estimated_arrival_date TIMESTAMP,     -- Estimated arrival for this leg
  transit_time_in_hours DECIMAL,        -- Expected transit time
  
  -- Request information
  request_timestamp TIMESTAMP,          -- When the leg was requested
  request_id VARCHAR,                   -- Request identifier
  
  -- Creation metadata
  dw_creation_date TIMESTAMP            -- Data warehouse creation timestamp
);

-- Package Systems Event Table: Package status events
CREATE TABLE package_systems_event_na (
  -- Key identifiers
  package_id VARCHAR,                   -- Package identifier
  forward_tracking_id VARCHAR,          -- Forward tracking number
  
  -- Location information
  state_location_id VARCHAR,            -- Location where event occurred
  state_location_destination_id VARCHAR,-- Destination location
  
  -- Status information
  state_status VARCHAR,                 -- Current status of the package
  state_sub_status VARCHAR,             -- Sub-status details
  state_time TIMESTAMP,                 -- When status was recorded
  
  -- Creation metadata
  dw_created_time TIMESTAMP             -- Data warehouse creation timestamp
);

-- Shipment Timeline Table: Consolidated view of shipment events
-- (Generated from the SQL query)
CREATE TABLE shipment_timeline_enriched (
  -- Identifiers
  shipment_id VARCHAR,                  -- Shipment identifier
  package_id VARCHAR,                   -- Package identifier
  tracking_id VARCHAR,                  -- Tracking identifier
  vrid VARCHAR,                         -- Vehicle route ID
  
  -- Event information
  event_timestamp TIMESTAMP,            -- When the event occurred
  event_type VARCHAR,                   -- Type of event
  status_code VARCHAR,                  -- Status code
  source_system VARCHAR,                -- Source system (SLAM_LEG, GMP, PSE)
  
  -- Location information
  location_id VARCHAR,                  -- Location of the event
  destination_id VARCHAR,               -- Destination location
  
  -- Region information
  origin_region VARCHAR,                -- Origin region
  origin_country VARCHAR,               -- Origin country
  origin_timezone VARCHAR,              -- Origin timezone
  destination_region VARCHAR,           -- Destination region
  destination_country VARCHAR,          -- Destination country
  destination_timezone VARCHAR,         -- Destination timezone
  
  -- Timing information
  hours_since_previous_event DECIMAL,   -- Hours since the previous event
  event_sequence INTEGER,               -- Sequence of event in timeline
  
  -- Vehicle routing information
  lane VARCHAR,                         -- Origin-destination lane
  miles DECIMAL,                        -- Distance in miles
  
  -- Estimated vs actual arrival/departure
  origin_scheduled_arrival TIMESTAMP,   -- Scheduled arrival at origin
  origin_calc_arrival TIMESTAMP,        -- Actual arrival at origin
  origin_arrival_late_hrs DECIMAL,      -- Hours late at origin
  origin_scheduled_depart TIMESTAMP,    -- Scheduled departure from origin
  origin_calc_depart TIMESTAMP,         -- Actual departure from origin
  origin_depart_late_hrs DECIMAL,       -- Hours late departing origin
  dest_scheduled_arrival TIMESTAMP,     -- Scheduled arrival at destination
  dest_calc_arrival TIMESTAMP,          -- Actual arrival at destination
  dest_arrival_late_hrs DECIMAL         -- Hours late at destination
);

-- Shipment Journey Analysis: Aggregate metrics on shipment journeys
-- (Generated from the SQL query)
CREATE TABLE shipment_journey_analysis (
  -- Identifiers
  shipment_id VARCHAR,                  -- Shipment identifier
  package_id VARCHAR,                   -- Package identifier
  tracking_id VARCHAR,                  -- Tracking identifier
  
  -- Journey metrics
  first_event_time TIMESTAMP,           -- Time of first event
  last_event_time TIMESTAMP,            -- Time of last event
  total_journey_hours DECIMAL,          -- Total journey duration in hours
  total_events INTEGER,                 -- Total number of events
  
  -- Event counts by source system
  num_systems_involved INTEGER,         -- Number of systems with events
  slam_events INTEGER,                  -- Count of SLAM events
  gmp_events INTEGER,                   -- Count of GMP events
  pse_events INTEGER,                   -- Count of PSE events
  
  -- Event sequence
  event_sequence VARCHAR                -- Sequence of events (joined with arrows)
);