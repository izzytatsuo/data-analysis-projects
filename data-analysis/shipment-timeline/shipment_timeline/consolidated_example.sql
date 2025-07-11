DROP TABLE IF EXISTS consolidated;
CREATE TEMP TABLE consolidated AS (



SELECT
  COALESCE(SGV.pk, PSE.shipment_package_pk) as pk
, COALESCE(SGV.source, PSE.source) as source
, COALESCE(SGV.event_timestamp, PSE.state_time) as event_timestamp
, COALESCE(SGV.duplicate_rn, PSE.duplicate_rn) as duplicate_rn
, SGV.shipment_id
, SGV.package_id
, SGV.warehouse_id
, SGV.route_delivery_group
, SGV.ship_method
, SGV.ship_option
, SGV.carrier_name
, SGV.zone
, SGV.pickup_date
, SGV.estimated_arrival_date
, SGV.request_timestamp
, SGV.external_estimated_arrival_date
, SGV.leg_warehouse_id
, SGV.leg_request_timestamp
, SGV.delivery_group_pickup_date
, SGV.shipment_type
, SGV.sender_id
, SGV.tracking_id
, SGV.ship_track_event_code
, SGV.standard_carrier_alpha_code
, SGV.supplement_code
, SGV.tcda_container_id
, SGV.parent_container_id
, SGV.parent_container_type
, SGV.status_node_id
, SGV.load_id
, SGV.status_date
, SGV.status_date_timezone
, SGV.edi_standard_name
, SGV.status_code
, SGV.reason_code
, SGV.amazon_bar_code
, SGV.fulfillment_shipment_id
, SGV.gmp_estimated_arrival_date
, SGV.gmp_package_id
, SGV.amazon_reference_number
, SGV.additional_reference_number
, SGV.status_ref_target_type
, SGV.service_type
, SGV.dw_created_time
, SGV.status_date_rn
, SGV.vrid
, SGV.origin
, SGV.final_destination
, SGV.origin_type
, SGV.destination_type
, SGV.vr_create_date

, PSE."type"
, PSE.package_id as pse_package_id
, PSE.package_id_type
, PSE.forward_amazon_barcode
, PSE.forward_tracking_id
, PSE.forward_tcda_container_id
, PSE.state_location_type
, PSE.state_location_id
, PSE.state_location_destination_id
, PSE.state_location_source_id
, PSE.state_status
, PSE.state_time
, PSE.triggerer_id
, PSE.triggerer_id_type
, PSE.dw_created_time AS pse_dw_created_time -- i am nulling this for now but need to come up with a better way to handle this
, PSE.state_sub_status
, PSE.comp_type
, PSE.comp_reason
, PSE.comp_state
, PSE.reverse_amazon_barcode
, PSE.reverse_tcda_container_id
, PSE.reverse_tracking_id
, PSE.execution_id
, PSE.execution_id_type
, PSE.state_location_destination_type
, PSE.state_location_source_type
-- , PSE.shipment_package_pk
, PSE.state_time_rn
, PSE.forward_tcda_rn
-- , PSE.source AS pse_source
-- , PSE.duplicate_rn


FROM
    slam_gmp_vls SGV
FULL OUTER JOIN
    pse PSE
ON
    SGV.source = PSE.source




);
