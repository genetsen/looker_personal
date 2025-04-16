connection: "looker_bqproject"

# include all the views
include: "/views/**/*.view.lkml"

datagroup: main_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: main_default_datagroup

# explore: prisma_porcessed {}

# explore: basis_view {}

# explore: prisma_expanded_full {}

# explore: prisma_expanded_summary {}

# explore: basis_gsheet {}

# explore: prisma_expanded_full_view {}

# explore: utms_view {}

explore: dcm_placement_costs_derived {

  label: "MFT_data_working"

  join: prisma_porcessed {
    relationship: many_to_one
    type: left_outer
    sql_on: ${prisma_porcessed.package_id} = ${dcm_placement_costs_derived.package_id}  ;;
    view_label: "Prisma Metadata"
  }

  join: utms_view {
    relationship: many_to_one
    type: left_outer
    sql_on: ${dcm_placement_costs_derived.placement_id} = ${utms_view.prisma_id} ;;
  }

}
