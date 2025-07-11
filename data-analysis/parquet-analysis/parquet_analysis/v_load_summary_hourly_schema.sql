-- Schema for v_load_summary_hourly table
CREATE TABLE v_load_summary_hourly (
  vrid VARCHAR,                         -- Unique vehicle route ID
  unused_region_id VARCHAR,             -- Unused region identifier
  unused_report_day TIMESTAMP,          -- Unused report day timestamp
  report_week VARCHAR,                  -- Week of the report (e.g., W27)
  report_month VARCHAR,                 -- Month of the report (e.g., M07)
  program_code VARCHAR,                 -- Purpose code (EMPTY, XFER, SCIB, AMZL, etc.)
  carrier_manager VARCHAR,              -- Carrier manager
  tp_id VARCHAR,                        -- Transport plan ID
  tour_id VARCHAR,                      -- Tour identifier
  scac VARCHAR,                         -- Standard Carrier Alpha Code
  carrier_name VARCHAR,                 -- Name of the carrier
  subcarrier VARCHAR,                   -- Subcarrier code
  carrier_group VARCHAR,                -- Carrier group (e.g., ATS_BROKERAGE, ATS_DEDICATED)
  lane VARCHAR,                         -- Origin to destination route (e.g., DCL4->CLE2)
  stop_count INT,                       -- Number of stops
  account_id VARCHAR,                   -- Business purpose identifier
  shipment_mode VARCHAR,                -- Mode of shipment (TRUCKLOAD, INTERMODAL, LESS_THAN_TRUCKLOAD)
  miles INT,                            -- Distance in miles
  cpt TIMESTAMP,                        -- Critical pull time/commitment
  adhoc_load VARCHAR,                   -- Whether the load is adhoc
  equipment_type VARCHAR,               -- Type of equipment/vehicle
  transit_operator_type VARCHAR,        -- Type of driver operation (e.g., SINGLE_DRIVER)
  tr_id VARCHAR,                        -- Transport record ID
  crid VARCHAR,                         -- Customer reference ID
  canceled_load VARCHAR,                -- Whether load was canceled (TRUE/FALSE)
  canceled_date TIMESTAMP,              -- Date and time of cancellation
  cancelation_reason VARCHAR,           -- Reason for cancellation
  origin VARCHAR,                       -- Origin facility code
  origin_zip VARCHAR,                   -- Origin ZIP code
  origin_city VARCHAR,                  -- Origin city
  origin_state VARCHAR,                 -- Origin state
  origin_country VARCHAR,               -- Origin country
  origin_type VARCHAR,                  -- Type of origin facility
  origin_local_timezone VARCHAR,        -- Local timezone of origin
  final_destination VARCHAR,            -- Destination facility code
  dest_zip VARCHAR,                     -- Destination ZIP code
  dest_city VARCHAR,                    -- Destination city
  dest_state VARCHAR,                   -- Destination state
  dest_country VARCHAR,                 -- Destination country
  destination_type VARCHAR,             -- Type of destination facility
  dest_local_timezone VARCHAR,          -- Local timezone of destination
  manifest_base DECIMAL,                -- Base manifest cost
  manifest_fuel DECIMAL,                -- Fuel manifest cost
  manifest_total DECIMAL,               -- Total manifest cost
  total_invoice_amount DECIMAL,         -- Total invoice amount
  total_paid_amount DECIMAL,            -- Total paid amount
  total_accessorials DECIMAL,           -- Total accessorial charges
  estimated_cost_accrual DECIMAL,       -- Estimated cost accrual
  accrual_cost_source VARCHAR,          -- Source of accrual cost
  tour_day_rate DECIMAL,                -- Day rate for tour
  total_pkg_unit_count INT,             -- Total package/unit count
  total_cube DECIMAL,                   -- Total cubic volume
  pallet_count INT,                     -- Count of pallets
  gaylord_count INT,                    -- Count of gaylord containers
  cube_target_cubic_ft DECIMAL,         -- Target cubic feet
  global_dea_pkgs VARCHAR,              -- Global DEA packages
  transit_hours_actual DECIMAL,         -- Actual transit hours
  scheduled_transit_hours DECIMAL,      -- Scheduled transit hours
  origin_scheduled_arrival TIMESTAMP,   -- Scheduled arrival at origin
  origin_calc_arrival TIMESTAMP,        -- Calculated/actual arrival at origin
  origin_calc_arrival_source VARCHAR,   -- Source of origin arrival calculation
  origin_begin_loading_time TIMESTAMP,  -- Beginning of loading at origin
  origin_finish_loading_time TIMESTAMP, -- End of loading at origin
  origin_arrival_late_group VARCHAR,    -- Grouping for origin arrival lateness
  origin_arrival_late_hrs DECIMAL,      -- Hours late at origin arrival (negative is early)
  origin_responsible VARCHAR,           -- Responsible party at origin
  origin_arrival_reason VARCHAR,        -- Reason for arrival timing at origin
  origin_arrival_note VARCHAR,          -- Notes regarding origin arrival
  origin_scheduled_depart TIMESTAMP,    -- Scheduled departure from origin
  origin_calc_depart TIMESTAMP,         -- Calculated/actual departure from origin
  origin_calc_depart_source VARCHAR,    -- Source of origin departure calculation
  origin_departure_late_group VARCHAR,  -- Grouping for origin departure lateness
  origin_depart_late_hrs DECIMAL,       -- Hours late at origin departure (negative is early)
  origin_fc_delay_hours DECIMAL,        -- Hours of delay at origin facility
  dest_scheduled_arrival TIMESTAMP,     -- Scheduled arrival at destination
  dest_calc_arrival TIMESTAMP,          -- Calculated/actual arrival at destination
  dest_calc_arrival_source VARCHAR,     -- Source of destination arrival calculation
  dest_begin_unloading_time TIMESTAMP,  -- Beginning of unloading at destination
  dest_finish_unloading_time TIMESTAMP, -- End of unloading at destination
  late_to_destination_per_calc VARCHAR, -- Whether late to destination per calculation
  dest_arrival_late_group VARCHAR,      -- Grouping for destination arrival lateness
  dest_arrival_late_hrs DECIMAL,        -- Hours late at destination arrival (negative is early)
  dest_responsible VARCHAR,             -- Responsible party at destination
  dest_arrival_note VARCHAR,            -- Notes regarding destination arrival
  dest_arrival_reason VARCHAR,          -- Reason for arrival timing at destination
  trailer_id VARCHAR,                   -- Trailer identifier
  bobtail_trailer_id VARCHAR,           -- Bobtail trailer identifier
  driver_id VARCHAR,                    -- Driver identifier
  driver_id_2 VARCHAR,                  -- Secondary driver identifier
  arc_type VARCHAR,                     -- ARC type
  wims_load VARCHAR,                    -- Whether it's a WIMS load
  enrichment_flag VARCHAR,              -- Enrichment flag
  tem_owned VARCHAR,                    -- TEM owned indicator
  run_structure_id VARCHAR,             -- Run structure identifier
  oneday_core_pkgs VARCHAR,             -- One-day core packages
  trailer_ready_time TIMESTAMP,         -- Time trailer is ready
  rate_type VARCHAR,                    -- Rate type (PER_LOAD, PER_TRIP)
  origin_load_type VARCHAR,             -- Type of loading at origin
  dest_unload_type VARCHAR,             -- Type of unloading at destination
  drop_trailer_time TIMESTAMP,          -- Time of trailer drop
  resource_block_id VARCHAR,            -- Resource block identifier
  operator_id VARCHAR,                  -- Operator identifier
  container_program VARCHAR,            -- Container program
  containerized_pkgs DECIMAL,           -- Containerized packages
  gl_account VARCHAR,                   -- General ledger account
  vr_create_date TIMESTAMP,             -- Vehicle route creation date
  rlb_load VARCHAR,                     -- RLB load indicator
  plan_type VARCHAR,                    -- Plan type
  movement_type VARCHAR,                -- Movement type
  power_id VARCHAR,                     -- Power unit identifier
  tem_rsp_region VARCHAR,               -- TEM responsible region
  is_customer_facing VARCHAR,           -- Customer facing indicator
  vehicle_execution_status VARCHAR,     -- Execution status (COMPLETED, CANCELLED, etc.)
  facility_sequence VARCHAR,            -- Facility sequence
  dest_planned_arrival TIMESTAMP,       -- Planned arrival at destination
  region_id VARCHAR                     -- Region identifier
);