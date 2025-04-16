view: prisma_expanded_full {
  sql_table_name: `20250327_data_model.prisma_expanded_full` ;;

  dimension: ad_server_placement_id {
    type: number
    sql: ${TABLE}.ad_server_placement_id ;;
  }
  dimension: ad_size__asset_ {
    type: string
    sql: ${TABLE}.ad_size__asset_ ;;
  }
  dimension: advertiser_name {
    type: string
    sql: ${TABLE}.advertiser_name ;;
  }
  dimension: advertiser_short_name {
    type: string
    sql: ${TABLE}.advertiser_short_name ;;
  }
  dimension: audience {
    type: string
    sql: ${TABLE}.audience ;;
  }
  dimension: audience_type {
    type: string
    sql: ${TABLE}.audience_type ;;
  }
  dimension: buy_category {
    type: string
    sql: ${TABLE}.buy_category ;;
  }
  dimension: buy_type {
    type: string
    sql: ${TABLE}.buy_type ;;
  }
  dimension: campaign_friendly {
    type: string
    sql: ${TABLE}.campaign_friendly ;;
  }
  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }
  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }
  dimension: channel__if_buy_category__custom___45 {
    type: string
    sql: ${TABLE}.channel__if_buy_category__custom___45 ;;
  }
  dimension: channel_group {
    type: string
    sql: ${TABLE}.channel_group ;;
  }
  dimension: channel_raw {
    type: string
    sql: ${TABLE}.channel_raw ;;
  }
  dimension: click_through_url {
    type: string
    sql: ${TABLE}.click_through_url ;;
  }
  dimension: cost_method {
    type: string
    sql: ${TABLE}.cost_method ;;
  }
  dimension: daily_impressions {
    type: number
    sql: ${TABLE}.daily_impressions ;;
  }
  dimension: daily_spend {
    type: number
    sql: ${TABLE}.daily_spend ;;
  }
  dimension_group: date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.date ;;
  }
  dimension: days_in_out_of_home_end_date {
    type: string
    sql: ${TABLE}.days_in_out_of_home_end_date ;;
  }
  dimension: device_type {
    type: string
    sql: ${TABLE}.device_type ;;
  }
  dimension: dimension {
    type: string
    sql: ${TABLE}.dimension ;;
  }
  dimension_group: end_date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.end_date ;;
  }
  dimension: external_entity_id {
    type: string
    sql: ${TABLE}.external_entity_id ;;
  }
  dimension: funnel {
    type: string
    sql: ${TABLE}.funnel ;;
  }
  dimension: geo_market {
    type: string
    sql: ${TABLE}.geo_market ;;
  }
  dimension: initative {
    type: string
    sql: ${TABLE}.initative ;;
  }
  dimension: kpi {
    type: string
    sql: ${TABLE}.kpi ;;
  }
  dimension: line_item_name {
    type: string
    sql: ${TABLE}.line_item_name ;;
  }
  dimension: media_name {
    type: string
    sql: ${TABLE}.media_name ;;
  }
  dimension: media_type {
    type: string
    sql: ${TABLE}.media_type ;;
  }
  dimension: n_of_placements {
    type: number
    sql: ${TABLE}.n_of_placements ;;
  }
  dimension: out_of_home_end_date {
    type: string
    sql: ${TABLE}.out_of_home_end_date ;;
  }
  dimension: p_package_friendly {
    type: string
    sql: ${TABLE}.p_package_friendly ;;
  }
  dimension: package_group_name {
    type: string
    sql: ${TABLE}.package_group_name ;;
  }
  dimension: package_id {
    type: string
    sql: ${TABLE}.package_id ;;
  }
  dimension: package_name {
    type: string
    sql: ${TABLE}.package_name ;;
  }
  dimension: package_type {
    type: string
    sql: ${TABLE}.package_type ;;
  }
  dimension: payable_rate {
    type: number
    sql: ${TABLE}.payable_rate ;;
  }
  dimension: placement_cap_cost {
    type: string
    sql: ${TABLE}.placement_cap_cost ;;
  }
  dimension: placement_comments {
    type: string
    sql: ${TABLE}.placement_comments ;;
  }
  dimension: placement_id {
    type: string
    sql: ${TABLE}.placement_id ;;
  }
  dimension: placement_name {
    type: string
    sql: ${TABLE}.placement_name ;;
  }
  dimension: placement_name2 {
    type: string
    sql: ${TABLE}.placement_name2 ;;
  }
  dimension: placement_type {
    type: string
    sql: ${TABLE}.placement_type ;;
  }
  dimension: placement_type__site_ {
    type: string
    sql: ${TABLE}.placement_type__site_ ;;
  }
  dimension: planned_actions {
    type: number
    sql: ${TABLE}.planned_actions ;;
  }
  dimension: planned_amount {
    type: number
    sql: ${TABLE}.planned_amount ;;
  }
  dimension: planned_clicks {
    type: number
    sql: ${TABLE}.planned_clicks ;;
  }
  dimension: planned_cost_pk {
    type: number
    sql: ${TABLE}.planned_cost_pk ;;
  }
  dimension: planned_daily_impressions_pk {
    type: number
    sql: ${TABLE}.planned_daily_impressions_pk ;;
  }
  dimension: planned_daily_spend_pk {
    type: number
    sql: ${TABLE}.planned_daily_spend_pk ;;
  }
  dimension: planned_impressions {
    type: number
    sql: ${TABLE}.planned_impressions ;;
  }
  dimension: planned_imps_pk {
    type: number
    sql: ${TABLE}.planned_imps_pk ;;
  }
  dimension: planned_units {
    type: number
    sql: ${TABLE}.planned_units ;;
  }
  dimension: product_code {
    type: string
    sql: ${TABLE}.product_code ;;
  }
  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }
  dimension: provider_name {
    type: string
    sql: ${TABLE}.provider_name ;;
  }
  dimension_group: report {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
  }
  dimension_group: script_run {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.script_run_date ;;
  }
  dimension: served_by {
    type: string
    sql: ${TABLE}.served_by ;;
  }
  dimension_group: start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.start_date ;;
  }
  dimension: supplier_code {
    type: string
    sql: ${TABLE}.supplier_code ;;
  }
  dimension: supplier_name {
    type: string
    sql: ${TABLE}.supplier_name ;;
  }
  dimension: total_days {
    type: number
    sql: ${TABLE}.total_days ;;
  }
  dimension: unit_type {
    type: string
    sql: ${TABLE}.unit_type ;;
  }
  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
	package_group_name,
	media_name,
	advertiser_short_name,
	product_name,
	line_item_name,
	campaign_name,
	supplier_name,
	provider_name,
	placement_name,
	package_name,
	advertiser_name
	]
  }

}
