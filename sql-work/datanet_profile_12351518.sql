/*NO DEPENDENCIES*/
 
SET TIME ZONE 'America/Los_Angeles';
 
--drop table if exists mdm;  --- Should we not use this instead of all the joins we have with
create temp table mdm_{TEMPORARY_TABLE_SEQUENCE} diststyle all as
(
    select distinct location_id as station
                  , region
                  , country
    			  , mdm.timezone as ds_timezone
                  , (convert_timezone('UTC', mdm.timezone, to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS')::timestamp))::date as today_dstz
                  , convert_timezone(mdm.timezone, 'UTC', (TO_TIMESTAMP(today_dstz, 'YYYY/MM/DD HH24:MI:SS')::Date)::TIMESTAMP) as today_utc
    from andes.perfectmile.d_perfectmile_node_mapping_mdm mdm
    where (mdm.country in ('US', 'CA')
        and mdm.location_type IN ('DS')
        and mdm.location_status = 'A'
        and business in ('AMZL', 'AMZN', 'COLO')
        )
       or mdm.location_id = 'XVV2'
);
 
------
 
 
 
--drop table if exists slam_leg;
create temp table slam_leg_{TEMPORARY_TABLE_SEQUENCE} distkey(shipment_id) as
    (
        select a.*
        from (
       		 select  shipment_id ,
        			 zone,
        	  		 max(pickup_date) over (partition by shipment_id) as ds_cpt_1,
                     row_number() over (partition by shipment_id ORDER BY pickup_date DESC, estimated_arrival_date DESC) AS rk
       		 from backlog_datasets.ATROPS.o_slam_packages_leg_live
        	 where request_timestamp >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 7
            		and leg_ship_method like 'AMZ%'
            		and ship_option not like '%mfn%'
            		and len(leg_warehouse_id) = 4
            		and (left(leg_warehouse_id,1) = 'D' or leg_warehouse_id = 'XVV2')
            )a
        join mdm_{TEMPORARY_TABLE_SEQUENCE} b
            on a.zone = b.station
        where rk=1
        and b.country in ('US', 'CA')
        and date(ds_cpt_1) between to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS')::date -10
            and to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS')::date + 5
    );
 
------
 
 
--drop table if exists slam;
create temp table slam_{TEMPORARY_TABLE_SEQUENCE} distkey(shipment_id) AS
(
    select slam_a.shipment_id
        , slam_a.station_code
        , slam_a.pickup_date_utc
        , slam_a.ead_utc
        , slam_a.slam_time_utc
        , slam_a.ship_method
        , slam_a.warehouse_id
        , slam_a.rn
        , convert_timezone('UTC', mdm.ds_timezone, pickup_date_utc) as pickup_date_dstz
        , convert_timezone('UTC', mdm.ds_timezone, ead_utc) as ead_dstz
        , convert_timezone('UTC', mdm.ds_timezone, slam_time_utc) as slam_time_dstz
        , mdm.ds_timezone
        , nvl(slam_a.internal_sort_code,'DS-A') as sort_code
    from
        (
            select ospl.shipment_id::varchar(256) as shipment_id
                   , ospl.zone as station_code
                   , ospl.pickup_date as pickup_date_utc
                   , ospl.estimated_arrival_date as ead_utc
                   , ospl.request_timestamp as slam_time_utc
                   , ospl.ship_method
                   , ospl.internal_sort_code
                   , ospl.warehouse_id
                   , row_number() over (partition by ospl.shipment_id ORDER BY ospl.pickup_date DESC, ospl.estimated_arrival_date DESC) AS rn
              from backlog_datasets.ATROPS.o_slam_packages_live ospl
              where ospl.request_timestamp >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 7
                and ospl.ship_method like 'AMZ%'
                AND ospl.ship_option not like '%mfn%'
                and len(ospl.zone) = 4
                and (left(ospl.zone, 1) = 'D' or ospl.zone = 'XVV2')
             ) slam_a
            inner join mdm_{TEMPORARY_TABLE_SEQUENCE} mdm
        on slam_a.station_code = mdm.station
    where rn = 1
);
 
------
 
 
 
create temp table sidelines_{TEMPORARY_TABLE_SEQUENCE} distkey(tracking_id) AS
(
    select distinct entity_id AS tracking_id
    from backlog_datasets.amzlcore.induct_events_na
    where entity_id_type = 'TRACKING_ID'
      and dw_created_time >= to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS') - 7
      and event_time <= to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS')
      and event_reason = 'INDUCT_SIDELINE'
);
 
------
 
 
--drop table if exists pse_latest;
create temp table pse_latest_{TEMPORARY_TABLE_SEQUENCE} DISTKEY (forward_tcda_container_id) as
(
    SELECT *
    FROM (
             SELECT forward_tracking_id             -- same as tracking_id
                  , forward_tcda_container_id       -- multiple tracking id into a contrain
                  , state_status
                  , state_sub_status
                  , state_time
                  , state_location_id
                  , state_location_source_type      -- delivery station
                  , state_location_destination_type -- customer address
                  , reverse_tracking_id             -- get generated rts, fc , cancel the order mid-way
                  , state_location_destination_id   -- new column for walker site exclusion
                  , ROW_NUMBER() OVER (PARTITION BY forward_tcda_container_id, forward_tracking_id ORDER BY state_time DESC, reverse_tracking_id ) AS rn
             FROM backlog_datasets.amzlcore.package_systems_event_na
             WHERE dw_created_time >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 7
         )
    WHERE rn = 1
);
 
------
 
create temp table gmp_latest_{TEMPORARY_TABLE_SEQUENCE} distkey(fulfillment_shipment_id)  sortkey ( rn, ship_track_event_code, dw_created_time, status_date ) as
(
select tcda_container_id
     , parent_container_id
     , tracking_id
     , fulfillment_shipment_id
     , status_node_id
     , ship_track_event_code
     , status_date
     , status_date_timezone
     , a.ship_method
     , a.supplement_code                      -- (@shshete 06/17/24)
     , dw_created_time
     , row_number() over (partition by a.tcda_container_id order by dw_created_time desc) as rn
from backlog_datasets.amzlcore.gmp_shipment_events_na a
    join slam_{TEMPORARY_TABLE_SEQUENCE} s on a.fulfillment_shipment_id = s.shipment_id
where dw_created_time >= to_timestamp(sysdate::timestamptz at time zone 'utc', 'YYYY/MM/DD HH24:MI:SS')::timestamp - 7
  -- AND ship_option not like 'same%'
  AND a.ship_option not like '%mfn%'
  -- AND UPPER(a.ship_method) like 'AMZL%'
  AND ship_track_event_code NOT IN ('EVENT_301', 'EVENT_302')
);
 
 
create temp table gmp_truck_checkin_2_{TEMPORARY_TABLE_SEQUENCE} distkey (fulfillment_shipment_id) as
(
    with t1 as (
        select   status_node_id
               , status_date
               , status_date_timezone
               , tracking_id
               , fulfillment_shipment_id
               , parent_container_id
        	   , mdm.ds_timezone
               , convert_timezone(status_date_timezone, mdm.ds_timezone, status_date) as status_date_dstz
               , row_number() over (partition by tcda_container_id order by dw_created_time desc) as rn
        from gmp_latest_{TEMPORARY_TABLE_SEQUENCE} a
        left join mdm_{TEMPORARY_TABLE_SEQUENCE} mdm
        	on a.status_node_id = mdm.station
        where ship_track_event_code in ('EVENT_253') AND supplement_code = 'GEOFENCE_CHECKIN'       -- (@shshete 06/17/24)
	)
    select * from t1 where rn = 1
);
 
------
 
 
create temp table node_type_{TEMPORARY_TABLE_SEQUENCE} distkey (node_id) sortkey(node_id) as
(
    SELECT distinct node_id, node_type
    FROM andes.perfectmile.d_perfectmile_node_package_v2_na
    WHERE (
           estimated_arrival_datetime >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 7 OR
           ship_datetime_utc >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 7
          )
      AND outer_ship_method LIKE 'AMZL%'
      AND country IN ('US', 'CA')
      AND (delivery_station_code LIKE 'D%' or delivery_station_code = 'XVV2')
      AND partition_date >= TO_TIMESTAMP(SYSDATE::TIMESTAMPTZ AT TIME ZONE 'utc', 'YYYY/MM/DD HH24:MI:SS')::TIMESTAMP - 20
      AND node_Type in ('AA','SC','FC','DS')
);
 
------
 
create temp table all_pkgs_{TEMPORARY_TABLE_SEQUENCE} distkey(tracking_id) sortkey(state_status, shipment_id, station_code) as
(
    select *
    from (
             select b.tcda_container_id
                  , g.parent_container_id
                  , b.tracking_id
                  , c.shipment_id
                  , b.ship_method
                  , c.sort_code
                  , case
                        when b.tracking_id is null and slam_time_utc is not null then 'MANIFESTED'
                        when b.tracking_id <> a.forward_tracking_id or b.ship_method not like 'AMZ%' then 'RE-SLAM'
                        when a.state_status = 'RECEIVED' and a.state_location_id <> c.station_code and REVERSE_TRACKING_ID is null then 'MANIFESTED'
                        when a.state_status = 'RECEIVED' and a.state_location_id = c.station_code and REVERSE_TRACKING_ID is null then 'IN_YARD'
                        else STATE_STATUS end as state_status
                  , state_sub_status
                  , convert_timezone('UTC', c.ds_timezone, a.state_time) as state_time_dstz
                  , b.ship_track_event_code
                  , a.state_time as state_time_utc
                  , c.warehouse_id
                  , d.location_type as warehouse_id_type
                  , a.state_location_id
                  , c.station_code
                  , c.ds_timezone
                  , c.pickup_date_dstz
                  , c.pickup_date_utc
                  , c.ead_dstz
                  , c.ead_utc
                  , a.STATE_LOCATION_SOURCE_TYPE
                  , a.STATE_LOCATION_DESTINATION_TYPE
                  , a.REVERSE_TRACKING_ID
                  , convert_timezone('UTC', c.ds_timezone, z.ds_cpt_1) as ds_cpt_dstz
                  , c.slam_time_utc               --(@shshete 06/17/24)
                  , c.slam_time_dstz              --(@shshete 06/17/24)
                  , g.status_date_dstz as truck_checkin_time_dstz
                  , a.state_location_destination_id  -- new column for walker site exclusion
             from slam_{TEMPORARY_TABLE_SEQUENCE} c
                      left join slam_leg_{TEMPORARY_TABLE_SEQUENCE} z
                               on c.shipment_id = z.shipment_id and  c.station_code= z.zone
                      left join gmp_latest_{TEMPORARY_TABLE_SEQUENCE} b
                                on b.rn = 1
                                    and b.fulfillment_shipment_id = c.shipment_id
                      left join pse_latest_{TEMPORARY_TABLE_SEQUENCE} a
                                on a.forward_tcda_container_id = b.tcda_container_id -- should be unique --
                      left join andes.perfectmile.d_perfectmile_node_mapping_mdm d
                                on c.warehouse_id::varchar = d.location_id::varchar
                      left join gmp_truck_checkin_2_{TEMPORARY_TABLE_SEQUENCE} g
		                    	on g.tracking_id = b.tracking_id
         )
    where STATE_STATUS <> 'RE-SLAM' -- slam again once , everything might changed --
);
 
------
 
 
create temp table station_ops_clock_{TEMPORARY_TABLE_SEQUENCE} distkey(station_code) sortkey(rnk)  as
(
    SELECT *
         , DENSE_RANK() OVER (PARTITION BY station_code,ofd_date ORDER BY inbound_end DESC) as rnk
    FROM (
             SELECT region
                  , location_super_region
                  , station_code
                  , cycle
                  , cast(ofd_date as date)                                      as ofd_date
                  , b.timezone
                  , a.inbound_end
                  , max(concat(cast(ofd_date as date), ' 08:00:00')::timestamp) as cet
                  , max(scms_sort_end)                                          as scms_sort_end -- DS sort_end_time
                  , max(convert_timezone(b.timezone, 'UTC', scms_sort_end))     as scms_inductsort_end_utc
                  , max(scms_pickstage_end)                                     as scms_pickstage_end
                  , max(scms_dispatch_dsp_start)                                as scms_dispatch_dsp_start
                  , max(scms_dispatch_dsp_end)                                  as scms_dispatch_dsp_end
             from andes.amzl_analytics.scms_ops_clock as a
                      left join andes.perfectmile.d_perfectmile_node_mapping_mdm as b
                                on a.station_code = b.location_id
             WHERE ofd_date between current_date - 8 and current_date+4
               AND a.MASTER_REGION = 'NA'
               and lower(cycle) in ('cycle_1', 'cycle_2', 'cycle_0')
             group by 1, 2, 3, 4, 5, 6, 7
         )
);
 
------
 
 
create temp table v_load_summary_{TEMPORARY_TABLE_SEQUENCE} distkey(vrid) sortkey(final_destination) as
(
    select vrid
       , origin
       , final_destination
       , origin_type
       , destination_type
       , lane
       , cpt
       , miles
       , origin_local_timezone
       , origin_scheduled_arrival
       , origin_calc_arrival
       , origin_arrival_late_group
       , origin_arrival_late_hrs
       , origin_arrival_reason
       , origin_scheduled_depart
       , origin_calc_depart
       , origin_departure_late_group
       , origin_depart_late_hrs
       , dest_local_timezone
       , dest_scheduled_arrival
       , dest_calc_arrival
       , dest_arrival_late_group
       , dest_arrival_late_hrs
       , dest_arrival_reason
       , account_id
    from andes."ats-onestopshop".v_load_summary_hourly
    where region_id = 'NA'
      and dest_country IN ('US', 'CA')
      and report_day between current_date - 20 and current_date -- is there any issue in adding the date range?
);
 
------
 
create temp table gmp_latest_2_{TEMPORARY_TABLE_SEQUENCE} distkey(tracking_id) sortkey(status_node_id) as
(
    SELECT *
    FROM (
        SELECT status_node_id
        	, ship_track_event_code
       		, tracking_id
       		, tcda_container_id
       		, parent_container_id
       		, ROW_NUMBER() OVER (PARTITION BY tcda_container_id ORDER BY status_date DESC) AS rn
       FROM gmp_latest_{TEMPORARY_TABLE_SEQUENCE}
       WHERE
            (
                ship_track_event_code = 'EVENT_253'
                OR ship_track_event_code = 'EVENT_254'
            )      -- (@shshete 06/17/24)
      )
	WHERE rn = 1
);
 
------
 
 
 
create temp table upstream_{TEMPORARY_TABLE_SEQUENCE} distkey(tracking_id) sortkey(at_upstream_node, destination_type) AS
(
    SELECT a.tcda_container_id
    , a.tracking_id
    , a.warehouse_id
    , a.warehouse_id_type
    , b.status_node_id  -- changed from coalesce(b.status_node_id, state_location_id)  to status_node_id (shshete@)
    , e.final_destination
    , d.node_type  as destination_type
    , e.origin
    , c.node_type  as origin_type   -- changed from  coalesce(c.node_type, f.node_type)  to c.node_type (shshete@)
    , e.vrid
    , e.lane
    , e.cpt
    , e.miles
    , e.origin_local_timezone
    , e.origin_scheduled_arrival
    , e.origin_calc_arrival
    , e.origin_arrival_late_group
    , e.origin_arrival_late_hrs
    , e.origin_arrival_reason
    , e.origin_scheduled_depart
    , e.origin_calc_depart
    , e.origin_departure_late_group
    , e.origin_depart_late_hrs
    , e.dest_local_timezone
    , e.dest_scheduled_arrival
    , e.dest_calc_arrival
    , e.dest_arrival_late_group
    , e.dest_arrival_late_hrs
    , e.dest_arrival_reason
    , f.node_type as status_node_type   -- (shshete)
    , case when e.cpt <= o.inbound_end then o.inbound_end
		when e.cpt >   o.inbound_end  then dateadd(day, 1, o.inbound_end)
    	else o.inbound_end end				                                as ds_cet
    , trunc(ds_cet)                                                         as ds_cet_date
    , a.station_code
    , case when a.ead_dstz >= o.scms_dispatch_dsp_start and  a.ead_dstz <= scms_dispatch_dsp_end then o.cycle else 'OTHERS' end as cycle
    , a.truck_checkin_time_dstz as status_date_dstz
    --, convert_timezone(g.status_date_timezone, a.ds_timezone, g.status_date) as status_date_dstz
    , SUM(CASE
          WHEN b.ship_track_event_code in ('EVENT_253') AND a.station_code != b.status_node_id    -- reference from a.ship_track_event_code to b.ship_track_event_code (shshete@)
          THEN 1
          ELSE 0 END)                                                    AS at_upstream_node
    , SUM(CASE
          WHEN b.ship_track_event_code in ('EVENT_254') AND a.station_code != b.status_node_id    -- reference from a.ship_track_event_code to b.ship_track_event_code (shshete@)
          THEN 1
          ELSE 0 END)                                                    AS in_transit_trailer
    , SUM(case
          when b.ship_track_event_code in ('EVENT_253', 'EVENT_254') AND                          -- reference from a.ship_track_event_code to b.ship_track_event_code (shshete@)
          a.station_code = b.status_node_id THEN 1
          else 0 end)                                                    AS in_yard_station
    FROM all_pkgs_{TEMPORARY_TABLE_SEQUENCE} a
    INNER JOIN gmp_latest_2_{TEMPORARY_TABLE_SEQUENCE} b
    ON a.state_status = 'MANIFESTED'
    and a.tracking_id = b.tracking_id
    and a.tcda_container_id = b.tcda_container_id
--     left join gmp_truck_checkin_2_{TEMPORARY_TABLE_SEQUENCE} g
--     on a.shipment_id = g.fulfillment_shipment_id
    left join v_load_summary_{TEMPORARY_TABLE_SEQUENCE} e -- truck id, gmp is not telling you where the truck going only about the status --
    on b.parent_container_id = e.vrid
    left join node_type_{TEMPORARY_TABLE_SEQUENCE} as c
    on e.origin = c.node_id -- (shshete@)
    left join node_type_{TEMPORARY_TABLE_SEQUENCE} as f   -- (shshete@)
    on b.status_node_id = f.node_id     -- (shshete@)
    left join node_type_{TEMPORARY_TABLE_SEQUENCE} as d
    on e.final_destination = d.node_id
    left join station_ops_clock_{TEMPORARY_TABLE_SEQUENCE} o
    on a.station_code = o.station_code
    and trunc(a.ead_dstz) = o.ofd_date
    and o.rnk = 1
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33,34,35
);
 
-------
 
 
create temp table backlog_final_status_pre_pre_{TEMPORARY_TABLE_SEQUENCE} diststyle even sortkey(state_sub_status, state_status_pre ) as
(
select distinct
               mdm.country
             , mdm.region
             , a.tcda_container_id
             , u.vrid       as vrid
             , a.tracking_id
             , a.shipment_id
             , a.ship_method
             , a.sort_code
             , u.cycle
             , u.cpt
             , u.lane
             , u.miles
             , u.origin
             , u.origin_type
             , u.final_destination
             , u.destination_type
             , a.warehouse_id
             , a.warehouse_id_type
             , u.origin_local_timezone
             , u.origin_scheduled_arrival
             , u.origin_calc_arrival
             , u.origin_arrival_late_group
             , u.origin_arrival_late_hrs
             , u.origin_arrival_reason
             , u.origin_scheduled_depart
             , u.origin_calc_depart
             , u.origin_departure_late_group
             , u.origin_depart_late_hrs
             , u.dest_local_timezone
             , u.dest_scheduled_arrival
             , u.dest_calc_arrival
             , u.dest_arrival_late_group
             , u.dest_arrival_late_hrs
             , u.dest_arrival_reason
             , a.station_code
             , a.ds_timezone
             , a.pickup_date_dstz
             , a.ds_cpt_dstz
             , a.ead_dstz
             ,coalesce(u.ds_cet,
                       (case when v.cpt <= o.inbound_end then o.inbound_end
							 when v.cpt >   o.inbound_end then dateadd(day, 1, o.inbound_end)
							 else  o.inbound_end end)
                      )
                    as ds_cet_datetime
             , case when u.vrid is not null then u.status_node_id
                    else g.status_node_id  end as status_node_id
             , a.ship_track_event_code
             , a.state_time_utc
             , a.state_time_dstz
             , a.reverse_tracking_id
             , u.status_date_dstz
             , case
                   when a.tracking_id in (select tracking_id from sidelines_{TEMPORARY_TABLE_SEQUENCE}) then 'SIDELINED'
 
                    when a.state_status in
                        ('MARKED_AS_LOST', 'DISPOSED', 'MARKED_AS_MISSING', 'MARKED_FOR_PROBLEM')
                        or (a.state_sub_status = 'DAMAGED' and REVERSE_TRACKING_ID is not null)
                        or (state_status = 'STOWED' and state_sub_status = 'FC_RETURN') then 'UNDELIVERABLE'
 
                    when (a.state_status = 'IN_TRANSIT' and
                          (a.STATE_LOCATION_DESTINATION_TYPE = 'MIDDLE_MILE_NODE' or
                           a.state_sub_status in ('CARRIER_SWITCHED', 'WRONG_CARRIER'))) then 'DEPARTED_FOR_FC'
 
                    when state_status in ('MARKED_FOR_REPROCESS','MANIFESTED') and REVERSE_TRACKING_ID is not null then 'FC_RETURN'
 
                    when state_status in ('STAGE_BUFFERED')and REVERSE_TRACKING_ID is not null then 'FC_RETURN'
 
                    when (state_status = 'MARKED_FOR_REPROCESS' and state_sub_status = 'CUSTOMER_CANCELLATION') then 'CANCELLATION'
 
                    when a.state_status = 'IN_TRANSIT'
                        and a.STATE_LOCATION_DESTINATION_TYPE = 'LAST_MILE_NODE'
                        and a.STATE_LOCATION_SOURCE_TYPE = 'LAST_MILE_NODE'
                        and a.state_location_destination_id !~ '^[A-Za-z0-9]{8,}$'   -- new column for walker site exclusion
                        then 'IN_TRANSIT_TO_ANOTHER_DS'
 
                    when a.state_status = 'IN_TRANSIT'
                        and a.STATE_LOCATION_DESTINATION_TYPE = 'LAST_MILE_NODE'
                        and a.STATE_LOCATION_SOURCE_TYPE = 'LAST_MILE_NODE'
                        and a.state_location_destination_id ~ '^[A-Za-z0-9]{8,}$'   -- new column for walker site exclusion
                        then 'DISPATCHED'
 
                    when (state_status in( 'MARKED_FOR_REPROCESS', 'DISPOSED', 'DELIVERY_FAILED', 'DELIVERY_REJECTED','DROP_FAILED') )
                        or (state_status = 'IN_TRANSIT' and REVERSE_TRACKING_ID is not null) then 'RTS'
 
                    when a.state_status = 'IN_TRANSIT'
                        and a.STATE_LOCATION_DESTINATION_TYPE in ('CUSTOMER_ADDRESS', 'LAST_MILE_ACCESS_POINT')
                    THEN 'DISPATCHED'
                    when a.STATE_STATUS IN ('PICKED', 'PICKED_FROM_BUFFER') THEN 'PICKED'
                    when a.state_status = 'DROPPED' THEN 'DELIVERED'
                    when a.state_status = 'STAGE_BUFFERED' THEN 'STOWED'
                    when a.state_status = 'STOW_BUFFERED' THEN 'INDUCTED'
                    when (a.state_status = 'MANIFESTED' and u.in_yard_station > 0)
                        or a.state_status = 'RECEIVED' then 'IN_YARD'
                    when a.state_status = 'MANIFESTED'
                        and u.at_upstream_node > 0
                        and u.status_node_type in ('FC') then 'AT_FC'
                    when a.state_status = 'MANIFESTED'
                        and u.at_upstream_node > 0
                        and u.status_node_type in ('SC') then 'AT_SC'
                    when a.state_status = 'MANIFESTED'
                        and u.at_upstream_node > 0
                        and u.status_node_type in ('AA') then 'AT_AH'
                    when a.state_status = 'MANIFESTED'
                        and u.at_upstream_node > 0 then 'AT_OTHER_UPSTREAM_NODES'
                    when a.state_status = 'MANIFESTED'
                        and u.in_transit_trailer > 0
                        and u.final_destination = a.station_code then 'IN_TRANSIT_TO_DS'
                    when a.state_status = 'MANIFESTED'
                        and u.in_transit_trailer > 0
                        and u.destination_type in ('FC') then 'IN_TRANSIT_TO_FC'
                    when a.state_status = 'MANIFESTED'
                        and u.in_transit_trailer > 0
                        and u.destination_type in ('SC') then 'IN
