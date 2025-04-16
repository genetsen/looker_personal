connection: "looker_bqproject"

include: "/views/*.view.lkml"                # include all views in the views/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
# explore: order_items {
#   join: orders {
#     relationship: many_to_one
#     sql_on: ${orders.id} = ${order_items.order_id} ;;
#   }
#
#   join: users {
#     relationship: many_to_one
#     sql_on: ${users.id} = ${orders.user_id} ;;
#   }
# }
explore: dcm_linked_view2 {
  join: prisma_porcessed {
    relationship: many_to_one
    type: left_outer
    sql_on: ${prisma_porcessed.package_id} = ${prisma_porcessed.package_id}  ;;
    view_label: "Prisma Metadata"
  }
}

explore: prisma_expanded_full_view {
  join:  dcm_linked_view2 {
    relationship: one_to_one
    type: full_outer
    sql_on: ${dcm_linked_view2.pris_exp_key} = ${prisma_expanded_full_view.primary_key}  ;;
  }
}

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
