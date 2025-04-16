view: utms_view {
  sql_table_name: `20250327_data_model.utms_view` ;;

  dimension: placement_creative {
    type: string
    sql: ${TABLE}.placement_creative ;;
  }
  dimension: placement_name {
    type: string
    sql: ${TABLE}.placement_name ;;
  }
  dimension: prisma_id {
    type: string
    sql: ${TABLE}.prisma_id ;;
  }
  dimension: scrap1 {
    type: string
    sql: ${TABLE}.Scrap1 ;;
  }
  dimension: scrap10 {
    type: string
    sql: ${TABLE}.scrap10 ;;
  }
  dimension: scrap2 {
    type: string
    sql: ${TABLE}.Scrap2 ;;
  }
  dimension: scrap3 {
    type: string
    sql: ${TABLE}.Scrap3 ;;
  }
  dimension: scrap4 {
    type: string
    sql: ${TABLE}.Scrap4 ;;
  }
  dimension: scrap5 {
    type: string
    sql: ${TABLE}.Scrap5 ;;
  }
  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }
  dimension: url_base {
    type: string
    sql: ${TABLE}.url_base ;;
  }
  dimension: url_scrap2 {
    type: string
    sql: ${TABLE}.url_scrap2 ;;
  }
  dimension: url_utms {
    type: string
    sql: ${TABLE}.url_utms ;;
  }
  dimension: urm_scrap {
    type: string
    sql: ${TABLE}.urm_scrap ;;
  }
  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }
  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }
  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }
  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }
  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }
  measure: count {
    type: count
    drill_fields: [placement_name]
  }
}
