view: prisma_expanded_full_view {
  sql_table_name: `20250327_data_model.prismaExpandedFull_view` ;;

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
  dimension: n_of_placements {
    type: number
    sql: ${TABLE}.n_of_placements ;;
  }
  dimension: p_package_friendly {
    type: string
    sql: ${TABLE}.p_package_friendly ;;
  }
  dimension: package_id {
    type: string
    sql: ${TABLE}.package_id ;;
  }
  dimension: placement_id {
    type: string
    sql: ${TABLE}.placement_id ;;
  }
  dimension: planned_amount {
    type: number
    sql: ${TABLE}.planned_amount ;;
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
  measure: count {
    type: count
  }
}
