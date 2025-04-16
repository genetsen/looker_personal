view: basis_gsheet {
  sql_table_name: `20250327_data_model.basis_gsheet` ;;

  dimension: basis_dsp_tactic_group {
    type: string
    sql: ${TABLE}.basis_dsp_tactic_group ;;
  }
  dimension: basis_tactic {
    type: string
    sql: ${TABLE}.basis_tactic ;;
  }
  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }
  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }
  dimension: creative_grouping_creative_grouping {
    type: string
    sql: ${TABLE}.creative_grouping_creative_grouping ;;
  }
  dimension: creative_name {
    type: string
    sql: ${TABLE}.creative_name ;;
  }
  dimension_group: day {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.day ;;
  }
  dimension: delivered_spend {
    type: number
    sql: ${TABLE}.delivered_spend ;;
  }
  dimension: gmail_dt {
    type: number
    sql: ${TABLE}.gmail_dt ;;
  }
  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }
  dimension: latest_record {
    type: number
    sql: ${TABLE}.latest_record ;;
  }
  dimension: line_item_name {
    type: string
    sql: ${TABLE}.line_item_name ;;
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
  dimension: placement {
    type: string
    sql: ${TABLE}.placement ;;
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
    drill_fields: [creative_name, line_item_name, campaign_name]
  }
}
