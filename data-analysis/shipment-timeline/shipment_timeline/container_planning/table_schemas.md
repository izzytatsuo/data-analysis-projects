# Container Planning Table Schemas

## amzl_routing2_container_pkg_na

This table tracks container package planning and induction information.

```sql
CREATE TABLE amzlcore.amzl_routing2_container_pkg_na (
    region_id integer ENCODE az64,
    country_code character varying(30) ENCODE lzo,
    station_code character varying(60) ENCODE lzo,
    originating_node character varying(65535) ENCODE lzo,
    originating_fc_or_sc character varying(6) ENCODE lzo,
    date timestamp without time zone ENCODE az64,
    cycle_name character varying(300) ENCODE lzo,
    dcap_run_time_local timestamp without time zone ENCODE az64,
    shipment_id bigint ENCODE az64,
    package_id integer ENCODE az64,
    total_units_in_pkg bigint ENCODE az64,
    container_plan_id character varying(150) ENCODE lzo,
    tracking_id character varying(300) ENCODE lzo distkey,
    is_planned integer ENCODE az64,
    is_inducted integer ENCODE az64,
    is_inducted_as_planned integer ENCODE az64,
    is_planned_not_inducted integer ENCODE az64,
    is_inducted_not_planned integer ENCODE az64,
    ship_method character varying(270) ENCODE lzo,
    induct_start_time timestamp without time zone ENCODE az64,
    induct_datetime_local timestamp without time zone ENCODE az64,
    induct_end_time timestamp without time zone ENCODE az64,
    slam_leg_sequence_id integer ENCODE az64,
    ds_inbound_vrid character varying(768) ENCODE lzo,
    slam_datetime_local timestamp without time zone ENCODE az64,
    actual_fc_departure_datetime_local timestamp without time zone ENCODE az64,
    actual_sc_departure_datetime_local timestamp without time zone ENCODE az64,
    actual_ds_arrival_datetime_local timestamp without time zone ENCODE az64,
    slam_estimated_arrival_datetime_local timestamp without time zone ENCODE az64,
    station_arrival_cutoff_local timestamp without time zone ENCODE az64,
    promised_arrival_datetime timestamp without time zone ENCODE az64,
    estimated_arrival_datetime timestamp without time zone ENCODE az64,
    cutoff_minus_eta bigint ENCODE az64,
    actual_arrival_minus_eta bigint ENCODE az64,
    dcap_minus_slam bigint ENCODE az64,
    condition character varying(22) ENCODE lzo,
    bag_or_ov character varying(8) ENCODE lzo,
    induct_sort_zone character varying(300) ENCODE lzo,
    stow_sort_zone character varying(144) ENCODE lzo,
    stow_datetime timestamp without time zone ENCODE az64,
    stow_dest_label character varying(768) ENCODE lzo,
    bag_firststow timestamp without time zone ENCODE az64,
    bag_rank bigint ENCODE az64,
    bag_rank_rev bigint ENCODE az64,
    is_double_binned character varying(6) ENCODE lzo,
    pkg_cubic_ft double precision ENCODE raw,
    pkg_lbs numeric(20,6) ENCODE az64,
    pkg_height numeric(12,2) ENCODE az64,
    pkg_width numeric(12,2) ENCODE az64,
    pkg_length numeric(12,2) ENCODE az64,
    pkg_uom character varying(768) ENCODE lzo,
    slam_oversize integer ENCODE az64,
    slam_oversize_criteria character varying(59) ENCODE lzo,
    first_cycle integer ENCODE az64,
    route_id character varying(20) ENCODE lzo,
    stop_number integer ENCODE az64,
    stop_packages bigint ENCODE az64,
    onzone_time numeric(18,2) ENCODE az64,
    service_time numeric(38,2) ENCODE az64,
    transit_time numeric(38,2) ENCODE az64,
    stow_compliance character varying(21) ENCODE lzo,
    max_sort_zones integer ENCODE az64,
    hard_cap numeric(38,8) ENCODE az64,
    soft_cap numeric(38,8) ENCODE az64,
    forecasted_shipments integer ENCODE az64,
    rgu_id character varying(300) ENCODE lzo,
    unit_place_id character varying(300) ENCODE lzo,
    building_place_id character varying(300) ENCODE lzo,
    bag_size numeric(3,2) ENCODE az64
)
DISTSTYLE KEY
SORTKEY ( region_id, date );
```

### Key Fields:
- `tracking_id`: Primary distribution key for the table
- `container_plan_id`: Container planning identifier
- `is_planned`, `is_inducted`: Flags indicating planning and induction status
- `is_inducted_as_planned`: Flag indicating if package was inducted according to plan
- `slam_datetime_local`: When the package was processed by SLAM
- `slam_estimated_arrival_datetime_local`: Estimated arrival time from SLAM
- `actual_ds_arrival_datetime_local`: Actual arrival time at delivery station