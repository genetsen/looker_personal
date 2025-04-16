view: basis_view {
  sql_table_name: `20250327_data_model.basis_view` ;;

  dimension: basis_dsp_tactic_group {
    type: string
    sql: ${TABLE}.basis_dsp_tactic_group ;;
  }
  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }
  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }
  dimension: creative_grouping {
    type: string
    sql: ${TABLE}.creative_grouping ;;
  }
  dimension: creative_name {
    type: string
    sql: ${TABLE}.creative_name ;;
  }
  dimension_group: date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.date ;;
  }
  dimension: gmail_dt {
    type: number
    sql: ${TABLE}.gmail_dt ;;
  }
  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }
  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
  }
  dimension: latest_record {
    type: number
    sql: ${TABLE}.latest_record ;;
  }
  dimension: media_cost {
    type: number
    sql: ${TABLE}.media_cost ;;
  }
  dimension: meta_data_date_pull {
    type: string
    sql: ${TABLE}.meta_data_date_pull ;;
  }
  dimension: meta_data_date_range {
    type: string
    sql: ${TABLE}.meta_data_date_range ;;
  }
  dimension: n {
    type: number
    sql: ${TABLE}.n ;;
  }
  dimension: package_roadblock {
    type: string
    sql: ${TABLE}.package_roadblock ;;
  }
  dimension: placement {
    type: string
    sql: ${TABLE}.placement ;;
  }
  dimension: tactic {
    type: string
    sql: ${TABLE}.tactic ;;
  }
  dimension: video_audio_fully_played {
    type: number
    sql: ${TABLE}.video_audio_fully_played ;;
  }
  dimension: video_audio_plays {
    type: number
    sql: ${TABLE}.video_audio_plays ;;
  }
  dimension: video_views {
    type: number
    sql: ${TABLE}.video_views ;;
  }
  dimension: viewable_impressions {
    type: number
    sql: ${TABLE}.viewable_impressions ;;
  }
  measure: count {
    type: count
    drill_fields: [creative_name]
  }
}
