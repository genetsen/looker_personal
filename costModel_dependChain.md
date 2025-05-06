```mermaid
%%  SHAPE LEGEND  ─────────────────────────
%%  ("text")   →  base_delivery_metadata  (rounded-rectangle)
%%  [text]     →  delivery_rollups        (sharp rectangle)
%%  ((text))   →  calculated_fields       (ellipse/circle)
%%  {text}     →  cost_model              (diamond)
%% ----------------------------------------

graph TD
  %% ───── base_delivery_metadata (bd.) ─────
  bd_flight_date(("bd flight_date_flag"))
  bd_flight_status(("bd flight_status_flag"))
  bd_rate_raw(("bd rate_raw"))

  %% ───── delivery_rollups (dr.) ─────
  dr_total_imps[[dr total_impressions]]
  dr_inflight_imps[[dr total_inflight_impressions]]
  dr_n_places[[dr n_of_placements]]
  dr_dmin[[dr d_min_date]]
  dr_dmax[[dr d_max_date]]
  dr_min_flight[[dr min_flight_date]]
  dr_max_flight[[dr max_flight_date]]

  %% ───── calculated_fields (cf.) ─────
  cf_pkg_daily_imps(((cf pkg_daily_imps)))
  cf_pkg_daily_perc(((cf pkg_daily_imps_perc)))
  cf_pkg_total_perc(((cf pkg_total_imps_perc)))
  cf_days_live(((cf days_live)))
  cf_prorated_cost(((cf prorated_planned_cost_pk)))
  cf_prorated_imps(((cf prorated_planned_imps_pk)))
  cf_cpm_over(((cf cpm_overdelivery_flag)))
  cf_daily_cpm(((cf daily_cpm)))

  %% ───── cost_model (cm.) ─────
  cm_cost{{cm daily_recalculated_cost}}
  cm_flag{{cm daily_recalculated_cost_flag}}
  cm_imps{{cm daily_recalculated_imps}}

  %% ───── DEPENDENCY EDGES ─────
  bd_rate_raw --> cm_cost
  bd_flight_date --> cm_cost

  dr_inflight_imps --> cm_cost
  dr_total_imps --> cf_pkg_total_perc
  dr_total_imps --> cf_cpm_over
  dr_total_imps --> cm_flag
  dr_total_imps --> cm_cost

  dr_dmin --> cf_days_live
  dr_dmax --> cf_days_live
  dr_min_flight --> cf_days_live
  dr_max_flight --> cf_days_live

  cf_pkg_daily_imps --> cf_pkg_daily_perc
  cf_pkg_daily_imps --> cm_cost

  cf_prorated_cost --> cm_cost
  cf_prorated_imps --> cm_cost
  cf_prorated_cost --> cm_flag

  cm_cost --> cm_flag
  cm_cost --> cm_imps
  ```
