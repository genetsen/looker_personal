# Prisma Planning Pipeline

This page is the short orientation guide for Prisma planning data in the master model. The detailed purpose, grain, inputs, outputs, refresh behavior, and risks for each live Prisma object are in the [Prisma table catalog](PRISMA_TABLE_CATALOG.md).

## What Prisma does

Prisma supplies package planning context: package and campaign identity, flight dates, planned spend, planned impressions, planned clicks, supplier metadata, channel context, and report-date freshness. Prisma planning is separate from delivered DCM and FPD metrics.

## Lineage at a glance

```text
Digital + offline Prisma reports
          ↓
[landing.prisma_master_2025]
          ├── process_prisma, daily 07:00 UTC
          │     ├── [prisma_porcessed]                    package grain
          │     ├── [prisma_porcessed_with_placements]    package/placement grain
          │     └── [prisma_processed_plusDCMimps]        package + DCM/FPD evidence
          │
          └── Prisma_expanded, Mon-Fri 07:00 UTC
                ├── [prisma_expanded_full]                package/date grain
                ├── [prisma_expanded_summary]              summary sibling
                └── [prismaExpanded_x_dcmDelivery]         planning + DCM delivery

[prisma_expanded_full]
          ↓
master-model Prisma CTEs
          ↓
master_stg.data_model and data_model_v3
```

The production object name `prisma_porcessed` contains a historical typo. Keep that name in SQL and links unless a coordinated migration is approved.

## Documentation map

- [Prisma table catalog](PRISMA_TABLE_CATALOG.md) — live object inventory and detailed object cards.
- [Master data model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md) — how Prisma planning becomes master-model fields.
- [Scheduled-query guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md) — refresh ownership, schedules, and operational checks.
- [DCM pipeline](../dcm/README_dcm-pipeline.md) — delivered DCM evidence.
- [FPD pipeline](../fpd/README_fpd-pipeline.md) — delivered FPD evidence.

## Core modeling rules

- Raw Prisma rows are report-row grain.
- `prisma_porcessed` is package grain.
- `prisma_porcessed_with_placements` is package/placement grain.
- `prisma_expanded_full` is package/date grain.
- Planned values describe what was bought or scheduled; DCM and FPD fields describe delivery evidence.
- A field missing from `prisma_processed_plusDCMimps` should be traced first through `prisma_porcessed`, before investigating the DCM or FPD joins.

## Quick troubleshooting

1. Check that both raw Prisma reports landed in [Prisma master 2025](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=prisma_master_2025&page=table).
2. Check the latest `process_prisma` or `Prisma_expanded` scheduled-query run.
3. Check the relevant grain: package, package/placement, or package/date.
4. Trace the field through the [Prisma table catalog](PRISMA_TABLE_CATALOG.md).
