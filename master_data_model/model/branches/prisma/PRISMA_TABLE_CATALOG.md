# Prisma Table Catalog

This catalog explains the purpose, grain, inputs, outputs, refresh behavior, and important risks of every current Prisma-named table or view found in the live warehouse. The inventory was verified on July 16, 2026.

## How to read this catalog

Each object uses the same compact card format:

- **Purpose** — why the object exists.
- **Grain** — what one row represents.
- **Reads from** — direct upstream inputs.
- **Feeds** — known downstream consumers.
- **Refresh** — how the object becomes current.
- **Risk** — naming, grain, freshness, or compatibility warnings.

## End-to-end lineage

```text
Prisma digital + offline reports
          ↓
landing.prisma_master_2025
          ├── process_prisma
          │     ├── 20250327_data_model.prisma_porcessed
          │     ├── 20250327_data_model.prisma_porcessed_with_placements
          │     ├── Prisma.prisma_processed_view
          │     ├── Prisma.prisma_processed_plusDCMimps
          │     └── Prisma.prisma_processed_plusDCMFPD
          │
          └── Prisma_expanded
                ├── 20250327_data_model.prisma_expanded_full
                ├── 20250327_data_model.prisma_expanded_summary
                ├── 20250327_data_model.prismaExpandedFull_view
                └── Prisma.prismaExpanded_x_dcmDelivery

20250327_data_model.prisma_expanded_full
          ↓
master-model Prisma CTEs
          ↓
master_stg.data_model and master_stg.data_model_v3
```

Both scheduled branches run around 07:00 UTC. If the raw landing table is current but a downstream object is stale, inspect the transfer run for the branch that owns that object.

## Live object cards

### Raw landing — `landing.prisma_master_2025`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=prisma_master_2025&page=table)

**Purpose**  Stores the latest imported digital and offline Prisma report rows.

**Grain**  One imported Prisma report row.

**Reads from**  Prisma email attachments processed by the Prisma loader.

**Feeds**  `process_prisma` and `Prisma_expanded`.

**Refresh**  Updated by the Prisma loader after the raw reports are collected and normalized.

**Important fields**  `Package_ID`, `CAMPAIGN_PUBLIC_ID`, `report_date`, `PLANNED_AMOUNT`, `PLANNED_IMPRESSIONS`, package metadata, campaign metadata, and flight dates.

**Risk**  This is planning input, not final delivery. Do not use it as proof that DCM or FPD delivered.

### Recovery snapshot — `landing.prisma_master_2025_recovered_2026_04_10`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=prisma_master_2025_recovered_2026_04_10&page=table)

**Purpose**  Preserves a recovered Prisma landing snapshot for investigation or comparison.

**Grain**  Historical Prisma report row.

**Feeds**  No current production branch is documented as reading this object.

**Refresh**  Recovery snapshot; not part of the routine loader path.

**Risk**  Treat as historical evidence, not current source of truth.

### Supporting asset table — `landing.prisma_supplier_logos`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=prisma_supplier_logos&page=table)

**Purpose**  Stores supplier logo assets used by Prisma-facing outputs or presentation layers.

**Grain**  Supplier logo record.

**Feeds**  Supplier-logo consumers; it is not part of the package planning metric path.

**Refresh**  Maintained by the Prisma supplier-logo reload workflow.

**Risk**  Do not include this asset table in planning or delivery reconciliation.

### Package processing — `20250327_data_model.prisma_porcessed`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_porcessed&page=table)

**Purpose**  Creates package-level Prisma planning records for package reporting and delivery joins.

**Grain**  One row per `package_id`.

**Reads from**  `landing.prisma_master_2025`.

**Feeds**  `prisma_processed_view`, `prisma_processed_plusDCMimps`, `prisma_processed_plusDCMFPD`, DCM joins, and related package-level Prisma outputs.

**Refresh**  Rebuilt by the `process_prisma` scheduled query at daily 07:00 UTC.

**Key transformations**  Excludes fee packages, aggregates placement rows to package grain, uses representative metadata aggregations, sums planned spend and impressions, and preserves `campaign_public_id`.

**Risk**  The production name contains the historical typo `porcessed`. Do not rename it casually.

### Placement processing — `20250327_data_model.prisma_porcessed_with_placements`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_porcessed_with_placements&page=table)

**Purpose**  Provides Prisma planning detail while retaining placement identity.

**Grain**  One row per package and placement.

**Reads from**  `landing.prisma_master_2025` through the placement branch of `process_prisma`.

**Feeds**  Placement-level Prisma consumers and investigation workflows.

**Refresh**  Rebuilt as the placement view statement in `process_prisma` runs.

**Risk**  This is a lower-grain object than `prisma_porcessed`; planned package metrics may repeat across placements and must not be summed without a grain rule.

### Processed compatibility view — `Prisma.prisma_processed_view`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=Prisma&t=prisma_processed_view&page=table)

**Purpose**  Provides a simple compatibility view over processed Prisma planning.

**Grain**  Inherits the package grain of `prisma_porcessed`.

**Reads from**  `20250327_data_model.prisma_porcessed`.

**Feeds**  Consumers that use the Prisma dataset namespace rather than the data-model namespace.

**Refresh**  Recomputed when queried from the current processed table.

### DCM/FPD enrichment — `Prisma.prisma_processed_plusDCMimps`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=Prisma&t=prisma_processed_plusDCMimps&page=table)

**Purpose**  Adds package-level DCM and FPD delivery evidence to processed Prisma planning.

**Grain**  One row per processed Prisma package.

**Reads from**  `prisma_porcessed`, DCM cost-model rollups, and FPD delivery rollups.

**Feeds**  Digital planning and delivery-status reporting.

**Refresh**  Recomputed when queried; freshness depends on all three source branches.

**Adds**  DCM impressions and date ranges, FPD impressions/spend/clicks and date ranges, tracking source, tracking status, and expected-impression calculations.

**Risk**  DCM and FPD joins add delivery fields but do not create or remove Prisma planning fields. If `campaign_public_id` is missing, check `prisma_porcessed` first.

### DCM/FPD sibling — `Prisma.prisma_processed_plusDCMFPD`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=Prisma&t=prisma_processed_plusDCMFPD&page=table)

**Purpose**  Provides the sibling package-level processed Prisma output with DCM and FPD rollups.

**Grain**  One row per processed Prisma package.

**Reads from**  `prisma_porcessed`, DCM cost-model rollups, and FPD delivery rollups.

**Feeds**  Package-level Prisma/FPD reporting consumers.

**Refresh**  Recomputed when queried from its upstream sources.

**Risk**  It shares the same processed-Prisma dependency and can become stale or incomplete when that upstream table is stale.

### Daily expansion — `20250327_data_model.prisma_expanded_full`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_expanded_full&page=table)

**Purpose**  Expands Prisma planning into daily package records for date-aligned model joins.

**Grain**  One row per package/date planning record.

**Reads from**  `landing.prisma_master_2025` through the `Prisma_expanded` scheduled query.

**Feeds**  The master-model Prisma CTEs and DCM/FPD planning joins.

**Refresh**  Rebuilt by `Prisma_expanded`, Monday through Friday at 07:00 UTC.

**Important fields**  `package_id`, `report_date`, `planned_daily_spend_pk`, `planned_daily_impressions_pk`, package metadata, and `campaign_public_id`.

**Risk**  This is the preferred daily planning source; downstream master projections still need to select any new field explicitly.

### Daily expansion compatibility view — `20250327_data_model.prismaExpandedFull_view`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prismaExpandedFull_view&page=table)

**Purpose**  Provides a view-compatible name over expanded Prisma planning.

**Grain**  Inherits the package/date grain of the expanded planning source.

**Feeds**  Compatibility consumers that use the view name.

**Refresh**  Recomputed from its underlying expanded source.

**Risk**  Confirm whether a consumer needs the current table or this compatibility view before changing either object.

### Expanded summary — `20250327_data_model.prisma_expanded_summary`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=20250327_data_model&t=prisma_expanded_summary&page=table)

**Purpose**  Stores a summarized sibling of the expanded Prisma planning output.

**Grain**  Summary grain defined by the live scheduled-query definition; verify before using for package/date reconciliation.

**Feeds**  Summary-oriented Prisma consumers.

**Refresh**  Maintained by the `Prisma_expanded` workflow.

**Risk**  Do not substitute this summary for `prisma_expanded_full` when daily planned metrics or field-level lineage are required.

### Expanded plus DCM delivery — `Prisma.prismaExpanded_x_dcmDelivery`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=Prisma&t=prismaExpanded_x_dcmDelivery&page=table)

**Purpose**  Stores expanded Prisma planning alongside DCM delivery evidence.

**Grain**  Package/date planning and DCM delivery grain defined by the live table build.

**Reads from**  Expanded Prisma planning and DCM delivery inputs.

**Feeds**  Prisma delivery comparison and reporting consumers.

**Refresh**  Built by the `Prisma_expanded` workflow.

**Risk**  DCM delivery fields may have a different natural grain than package/date planning; use the live definition before aggregating.

### Digital-plus-linear reporting view — `Prisma.prisma__stg__digital_plus_linear_view`

[Open in BigQuery](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=Prisma&t=prisma__stg__digital_plus_linear_view&page=table)

**Purpose**  Presents Prisma planning with digital-plus-linear delivery status and tracking evidence.

**Grain**  Package/date reporting grain.

**Reads from**  Processed Prisma planning plus DCM and FPD delivery sources.

**Feeds**  The digital-plus-linear snapshot and downstream planning reports.

**Refresh**  Recomputed by the digital-plus-linear scheduled workflow; see the [scheduled-query guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md).

**Important fields**  `total_dcm_impressions`, `tracking_status`, `tracking_delivery_source`, and package/date planning context.

## Shared field lineage

### Campaign identity

```text
Prisma reports
  → landing.prisma_master_2025.CAMPAIGN_PUBLIC_ID
  → process_prisma MAX(CAMPAIGN_PUBLIC_ID)
  → 20250327_data_model.prisma_porcessed.campaign_public_id
  → Prisma.prisma_processed_plusDCMimps.campaign_public_id
```

### Daily planned metrics

```text
landing.prisma_master_2025 planned fields
  → Prisma_expanded
  → 20250327_data_model.prisma_expanded_full
  → master-model prisma_daily and prisma_meta CTEs
  → master-model planned fields
```

### DCM and FPD delivery evidence

```text
DCM delivery + FPD delivery
  → package-level rollups
  → Prisma.prisma_processed_plusDCMimps
  → tracking source, tracking status, and delivery reporting
```

## Grain and join warnings

- Raw Prisma rows are report-row grain.
- `prisma_porcessed` is package grain.
- `prisma_porcessed_with_placements` is package/placement grain; planned package values can repeat across placements.
- `prisma_expanded_full` is package/date grain.
- DCM creative detail can be lower grain than package/date. Do not collapse it into package/date without an explicit aggregation rule.
- Planned metrics are not delivered metrics. DCM and FPD determine delivery evidence under their own documented precedence.

## Refresh and verification route

1. Confirm both raw Prisma reports reached [Prisma master 2025](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=landing&t=prisma_master_2025&page=table).
2. Check the `process_prisma` or `Prisma_expanded` transfer run that owns the missing object.
3. Check the object at its natural grain rather than relying on row count alone.
4. For a missing field, compare the schema and fill rate at the raw, processed, expanded, and enriched stages.
5. Use the [master-model Prisma branch guide](README_prisma-pipeline.md) for the handoff into `master_stg.data_model` and `data_model_v3`.

## Related documentation

- [Master data model README](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README.md)
- [Scheduled-query guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/docs/SCHEDULED_QUERIES.md)
- [DCM pipeline](../dcm/README_dcm-pipeline.md)
- [FPD pipeline](../fpd/README_fpd-pipeline.md)
