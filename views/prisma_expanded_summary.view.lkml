view: prisma_expanded_summary {
  sql_table_name: `20250327_data_model.prisma_expanded_summary` ;;

  dimension: daily_imps {
    type: number
    sql: ${TABLE}.daily_imps ;;
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
  dimension_group: end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.end_date ;;
  }
  dimension: p_package_friendly {
    type: string
    sql: ${TABLE}.p_package_friendly ;;
  }
  dimension: package_id {
    type: string
    sql: ${TABLE}.package_id ;;
  }
  dimension: package_name {
    type: string
    sql: ${TABLE}.package_name ;;
  }
  dimension: planned_amount {
    type: number
    sql: ${TABLE}.planned_amount ;;
  }
  dimension: planned_impressions {
    type: number
    sql: ${TABLE}.planned_impressions ;;
  }
  dimension_group: start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.start_date ;;
  }
  measure: count {
    type: count
    drill_fields: [package_name]
  }
}
