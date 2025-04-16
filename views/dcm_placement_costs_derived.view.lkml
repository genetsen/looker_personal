view: dcm_placement_costs_derived {
  derived_table: {
    persist_for: "24 hours"
    sql:
      WITH base_costs AS (
        SELECT
          package_id,
          placement_id,
          p_cost_method,
          rate_raw,
          p_pkg_total_planned_cost,
          p_pkg_total_planned_imps,
          p_total_days,
          date,

      SAFE_CAST(rate_raw AS NUMERIC) AS rate_numeric,
      SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) AS planned_cost_numeric,
      SAFE_CAST(p_total_days AS INT64) AS total_days_int,

      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(total_conversions) AS total_conversions,
      SUM(media_cost) AS cost,

      CASE
      WHEN p_cost_method = 'CPM' THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(impressions) / 1000
      WHEN p_cost_method = 'CPC' THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(clicks)
      WHEN p_cost_method = 'CPA' THEN SAFE_CAST(rate_raw AS NUMERIC) * SUM(total_conversions)
      WHEN p_cost_method = 'Flat' THEN
      CASE
      WHEN SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC) = 0 THEN SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) / SAFE_CAST(p_total_days AS NUMERIC)
      ELSE SAFE_CAST(p_pkg_total_planned_cost AS NUMERIC) * (SUM(impressions) / SAFE_CAST(p_pkg_total_planned_imps AS NUMERIC))
      END
      ELSE 0
      END AS daily_recalculated_cost
      FROM `looker-studio-pro-452620.DCM.dcm_linkedView2`
      GROUP BY
      package_id,
      placement_id,
      p_cost_method,
      rate_raw,
      p_pkg_total_planned_cost,
      p_pkg_total_planned_imps,
      p_total_days,
      date
      )
      
      SELECT *,
      SUM(daily_recalculated_cost) OVER (PARTITION BY package_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_recalculated_cost,

      CASE
      WHEN SUM(daily_recalculated_cost) OVER (PARTITION BY package_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= planned_cost_numeric THEN daily_recalculated_cost
      WHEN SUM(daily_recalculated_cost) OVER (PARTITION BY package_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) < planned_cost_numeric THEN planned_cost_numeric - SUM(daily_recalculated_cost) OVER (PARTITION BY package_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
      ELSE 0
      END AS placement_actualized_cost_by_day
      FROM base_costs
      ;;
  }

  dimension: package_id {
    type: string
    sql: ${TABLE}.package_id ;;
  }

  dimension: placement_id {
    type: string
    sql: ${TABLE}.placement_id ;;
  }

  dimension_group: date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.date ;;
  }

  dimension: placement_actualized_cost_by_day {
    type: number
    value_format_name: usd
    sql: ${TABLE}.placement_actualized_cost_by_day ;;
  }

  measure: placement_actualized_cost {
    type: sum
    value_format_name: usd
    sql: ${placement_actualized_cost_by_day} ;;
  }

  dimension: dcm_impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  measure: CPM_actualized {
    type: number
    value_format_name: usd
    sql: sum(${placement_actualized_cost_by_day})/sum(${TABLE}.impressions)*1000 ;;
  }
}
