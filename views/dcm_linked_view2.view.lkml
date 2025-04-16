view: dcm_linked_view2 {
  view_label: "dcm_linked_view2_orig"
  sql_table_name: `looker-studio-pro-452620.DCM.dcm_linkedView2` ;;

  dimension: ad {
    type: string
    sql: ${TABLE}.ad ;;
  }
  dimension: click_rate {
    type: number
    sql: ${TABLE}.click_rate ;;
  }
  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }
  dimension: creative {
    type: string
    sql: ${TABLE}.creative ;;
  }
  dimension_group: date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.date ;;
  }
  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }
  dimension: key {
    type: string
    sql: ${TABLE}.KEY ;;
  }
  dimension: media_cost {
    type: number
    sql: ${TABLE}.media_cost ;;
  }
  dimension: n_of_placements {
    type: number
    sql: ${TABLE}.n_of_placements ;;
  }
  dimension: p_cost_method {
    type: string
    sql: ${TABLE}.p_cost_method ;;
  }
  dimension_group: p_end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.p_end_date ;;
  }
  dimension: p_package_friendly {
    type: string
    sql: ${TABLE}.p_package_friendly ;;
  }
  dimension: p_pkg_daily_planned_cost {
    type: number
    sql: ${TABLE}.p_pkg_daily_planned_cost ;;
  }
  dimension: p_pkg_daily_planned_imps {
    type: number
    sql: ${TABLE}.p_pkg_daily_planned_imps ;;
  }
  dimension: p_pkg_total_planned_cost {
    type: number
    sql: ${TABLE}.p_pkg_total_planned_cost ;;
  }
  dimension: p_pkg_total_planned_imps {
    type: number
    sql: ${TABLE}.p_pkg_total_planned_imps ;;
  }
  dimension_group: p_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.p_start_date ;;
  }
  dimension: p_total_days {
    type: number
    sql: ${TABLE}.p_total_days ;;
  }
  dimension: package_id {
    type: string
    sql: ${TABLE}.package_id ;;
  }
  dimension: package_roadblock {
    type: string
    sql: ${TABLE}.package_roadblock ;;
  }
  dimension: pkg_daily_imps {
    type: number
    sql: ${TABLE}.pkg_daily_imps ;;
  }
  dimension: pkg_daily_imps_perc {
    type: number
    sql: ${TABLE}.pkg_daily_imps_perc ;;
  }
  dimension: pkg_total_imps {
    type: number
    sql: ${TABLE}.pkg_total_imps ;;
  }
  dimension: pkg_total_imps_perc {
    type: number
    sql: ${TABLE}.pkg_total_imps_perc ;;
  }
  dimension: placement_id {
    type: string
    sql: ${TABLE}.placement_id ;;
  }
  dimension: rate_raw {
    type: number
    sql: ${TABLE}.rate_raw ;;
  }
  dimension: rich_media_video_completions {
    type: number
    sql: ${TABLE}.rich_media_video_completions ;;
  }
  dimension: rich_media_video_plays {
    type: number
    sql: ${TABLE}.rich_media_video_plays ;;
  }
  dimension: test {
    type: string
    sql: ${TABLE}.test ;;
  }
  dimension: total_conversions {
    type: number
    sql: ${TABLE}.total_conversions ;;
  }

  dimension: rate_numeric {
    type: number
    sql: SAFE_CAST(${rate_raw} AS NUMERIC) ;;
  }

  dimension: pris_exp_key {
    type: string
    sql: CONCAT(CAST(${date_date} AS STRING), ${placement_id}) ;;
  }

  measure: count {
    type: count
  }

  measure: placement_recalculated_cost {
    type: number
    label: "Placement Recalculated Cost"
    value_format_name: usd
    sql: CASE
          WHEN min(${p_cost_method}) = 'CPM' THEN min(${rate_numeric}) * sum(${impressions}) / 1000
          # WHEN ${p_cost_method} = 'CPC' THEN ${rate_numeric} * ${clicks}
          # WHEN ${p_cost_method} = 'CPA' THEN ${rate_numeric} * ${total_conversions}
          # WHEN ${p_cost_method} = 'FLAT' THEN NULL  -- will be handled elsewhere
          ELSE NULL
        END ;;
  }

  measure: package_recalculated_cost_total {
    type: number
    label: "Package Recalculated Cost Total"
    value_format_name: usd
    sql: SUM(
          CASE
            WHEN MIN(${p_cost_method}) = 'CPM' THEN MIN(${rate_numeric}) * SUM(${impressions}) / 1000
            -- Add CPC, CPA back when needed
            ELSE NULL
          END
        ) ;;
  }

  measure: placement_actualized_cost {
    type: number
    label: "Placement Actualized Cost"
    value_format_name: usd
    sql: CASE
          WHEN SUM(${placement_recalculated_cost}) = 0 THEN NULL
          ELSE MIN(${p_pkg_total_planned_cost}) * (
            SUM(${placement_recalculated_cost}) / SUM(${package_recalculated_cost_total})
          )
        END ;;
  }




}
