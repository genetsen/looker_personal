# CodeViz Research Context

> **Note**: This file contains research context from CodeViz. Most recent contexts are at the bottom.

---

## Research Query

Investigate the "dcm" component in detail. Provide a comprehensive analysis of its architecture, responsibilities, and implementation details.

*Session: 4d8dc8480d25f967fdc1381df900ef87 | Generated: 7/7/2025, 4:49:11 PM*

### Analysis Summary

# DCM Component Analysis

The **DCM** component within this codebase primarily handles data related to DoubleClick Campaign Manager, focusing on cost modeling, delivery data, and its integration with other marketing data sources like UTMs and Prisma. It serves as a foundational data source for various marketing performance reports and analyses.

## High-Level Architecture

The **DCM** component is structured around raw data views, transformed SQL models, and Looker/Omni definitions that expose this data for reporting and analysis.

*   **Raw Data Views**: These are defined in YAML files and point to underlying BigQuery tables, providing a structured interface to the raw DCM data.
*   **SQL Transformations**: SQL scripts process and enrich the raw DCM data, often joining it with other datasets (e.g., UTMs, Prisma) to create unified delivery marts.
*   **Looker/Omni Models**: YAML files define Looker views and topics, building upon the SQL transformations to create a semantic layer for business users.

## Data Models and Views (Omni/Looker)

The `omni/bigquery_connection_v2/DCM/` directory contains several YAML files that define views and topics related to DCM data within the Looker/Omni environment. These files serve as the interface for Looker to interact with the underlying BigQuery tables.

### Core DCM Views

*   **`dcm_linkedview2.view.yaml`** [dcm_linkedview2.view.yaml](omni/bigquery_connection_v2/DCM/dcm_linkedview2.view.yaml):
    *   **Purpose**: Defines a Looker view for the `dcm_linkedView2` table in the `DCM` schema. This appears to be a primary source of raw DCM data.
    *   **Internal Parts**: Exposes fields from the underlying BigQuery table.
    *   **External Relationships**: Referenced by other views and topics, such as [omni__query.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query.query.view.yaml) and [omni__query_copy.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query_copy.query.view.yaml).

*   **`20240424_dcmcostmodel_v3.view.yaml`** [20240424_dcmcostmodel_v3.view.yaml](omni/bigquery_connection_v2/DCM/20240424_dcmcostmodel_v3.view.yaml):
    *   **Purpose**: Defines a Looker view for the `20240424_dcmCostmodel_v3` table in the `DCM` schema, likely representing a version of the DCM cost model.
    *   **Internal Parts**: Includes fields like `daily_recalculated_cost_flag`, `daily_recalculated_cost`, and `impressions`.
    *   **External Relationships**: Joined with `dcm_linkedview2` in [relationships.yaml](omni/bigquery_connection_v2/relationships.yaml).

*   **`dcm_linkedview.view.yaml`** [dcm_linkedview.view.yaml](omni/bigquery_connection_v2/DCM/dcm_linkedview.view.yaml):
    *   **Purpose**: Defines a Looker view for the `dcm_linkedView` table in the `DCM` schema. This might be an older or alternative linked view.

### Query Views

*   **`omni__query.query.view.yaml`** [omni__query.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query.query.view.yaml):
    *   **Purpose**: Defines a query view based on `dcm__dcm_linkedview2`, exposing specific fields for analysis.
    *   **Internal Parts**: Selects fields like `p_package_friendly` and `impressions` from the base view.
    *   **External Relationships**: Uses `dcm__dcm_linkedview2` as its `base_view`.

*   **`omni__query_copy.query.view.yaml`** [omni__query_copy.query.view.yaml](omni/bigquery_connection_v2/DCM/omni__query_copy.query.view.yaml):
    *   **Purpose**: Another query view, similar to `omni__query.query.view.yaml`, but potentially for different analytical needs.
    *   **Internal Parts**: Includes fields such as `p_cost_method`, `date`, `p_advertiser_name`, `package_id`, and `placement_id`.
    *   **External Relationships**: Also uses `dcm__dcm_linkedview2` as its `base_view` and references `dcm__20240424_dcmcostmodel_v3`.

### Topic Files

*   **`dcm__dcm_linkedview2.topic.yaml`** [dcm__dcm_linkedview2.topic.yaml](omni/bigquery_connection_v2/dcm__dcm_linkedview2.topic.yaml):
    *   **Purpose**: Defines a Looker topic based on `dcm__dcm_linkedview2`, providing a higher-level grouping of related fields for user exploration.
    *   **Internal Parts**: Sets the `base_view` to `dcm__dcm_linkedview2` and includes a reference to `dcm__20240424_dcmcostmodel_v3`.

### Data Model Views

*   **`dcm_test2.view.yaml`** [dcm_test2.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/dcm_test2.view.yaml):
    *   **Purpose**: A test view for DCM data within the `giant-spoon-299605.data_model_2025` schema.

*   **`latest_dcm_table.view.yaml`** [latest_dcm_table.view.yaml](omni/bigquery_connection_v2/giant-spoon-299605.data_model_2025/latest_dcm_table.view.yaml):
    *   **Purpose**: Defines a view for the `latest_dcm_table` within the `giant-spoon-299605.data_model_2025` schema, likely pointing to the most recent DCM data.

### Relationships

*   **`relationships.yaml`** [relationships.yaml](omni/bigquery_connection_v2/relationships.yaml):
    *   **Purpose**: Defines join relationships between different DCM-related views.
    *   **Internal Parts**: Specifies `join_from_view`, `join_to_view`, and `on_sql` conditions.
    *   **External Relationships**: Shows joins between `dcm__dcm_linkedview2` and `dcm__20240424_dcmcostmodel_v3`, and also with `prisma__prisma_processed`.

## SQL Transformations and Marts

The `sql/base/dcm/` directory contains SQL files responsible for transforming and preparing DCM data.

*   **`mart__delivery__join_dcm_utms_prisma.sql`** [mart__delivery__join_dcm_utms_prisma.sql](sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql):
    *   **Purpose**: This SQL script creates a unified delivery model by joining DCM data with UTM enrichment and potentially Prisma data. It aims to provide a comprehensive view of delivery metrics.
    *   **Internal Parts**: Selects numerous fields from the `dcm` alias (representing the `final_views.dcm` table) and joins with a `utm` alias. It also includes commented-out joins with Prisma data.
    *   **External Relationships**:
        *   **Source**: `looker-studio-pro-452620.final_views.dcm`
        *   **Target**: `looker-studio-pro-452620.repo_stg.dcm_plus_utms` (a view)
        *   **Joins**: `dcm` data is joined with UTM data based on `placement_id`, `creative`, and `ad_name`.

*   **`20250505_costModel_v5.sql`** [20250505_costModel_v5.sql](sql/base/dcm/20250505_costModel_v5.sql):
    *   **Purpose**: This script creates a specific version of the DCM cost model.
    *   **External Relationships**: Creates a table named `looker-studio-pro-452620.DCM.20250505_costModel_v5`.

### Marts

DCM data is integrated into various marketing marts:

*   **`mart__delivery__unified_dcm_basis.sql.sql`** [mart__delivery__unified_dcm_basis.sql.sql](sql/marts/delivery/mart__delivery__unified_dcm_basis.sql.sql):
    *   **Purpose**: Unifies DCM and Basis delivery data into a single schema using `UNION ALL`.
    *   **External Relationships**: Sources data from `looker-studio-pro-452620.repo_tables.dcm` and combines it with Basis data.

*   **`mart__mft.sql`** [mart__mft.sql](sql/marts/mft/mart__mft.sql):
    *   **Purpose**: Creates a unified delivery reporting table for MASS campaigns from DCM and Basis sources.
    *   **External Relationships**: Sources data from `looker-studio-pro-452620.repo_tables.dcm`.

*   **`mart__mft_view.sql`** [mart__mft_view.sql](sql/marts/mft/mart__mft_view.sql):
    *   **Purpose**: Similar to `mart__mft.sql`, but likely a view based on the staged `dcm_plus_utms` data.
    *   **External Relationships**: Sources data from `looker-studio-pro-452620.repo_stg.dcm_plus_utms`.

## Archival and Exploratory Files

*   **`archive/DCM/20250424_costModel_v2_repo.sql`** [20250424_costModel_v2_repo.sql](archive/DCM/20250424_costModel_v2_repo.sql):
    *   **Purpose**: An archived version of a DCM cost model.
    *   **External Relationships**: References `looker-studio-pro-452620.DCM.dcm_linkedView2`.

*   **`archive/DCM/dcm_costModel_v3.sql`** [dcm_costModel_v3.sql](archive/DCM/dcm_costModel_v3.sql):
    *   **Purpose**: Another archived version of a DCM cost model.
    *   **External Relationships**: References `looker-studio-pro-452620.DCM.dcm_linkedView2`.

*   **`Explorations/cost_model/costModel_tempTableUpdate.sql`** [costModel_tempTableUpdate.sql](Explorations/cost_model/costModel_tempTableUpdate.sql):
    *   **Purpose**: A script used for temporary table updates related to the cost model.
    *   **External Relationships**: Creates a table in the `DCM` schema: `DCM.cost_model_temp_scratch`.

## Testing and Validation

*   **`sql/base/dcm/query_validation_tests/test__costmodel__cost_underdelivery_unexplained_by_impressions.sql`** [test__costmodel__cost_underdelivery_unexplained_by_impressions.sql](sql/base/dcm/query_validation_tests/test__costmodel__cost_underdelivery_unexplained_by_impressions.sql):
    *   **Purpose**: A validation test for the cost model, specifically checking for cost underdelivery not explained by impressions.
    *   **External Relationships**: References `looker-studio-pro-452620.DCM.20250505_costModel_v5` and `looker-studio-pro-452620.dcm_costModel_scratch.20250504_dcmCostModel_v4_Scratch`.

## External Relationships

The **DCM** component is frequently integrated with other data sources:

*   **UTMs**: DCM data is joined with UTM (Urchin Tracking Module) data for campaign tracking and attribution, as seen in [mart__delivery__join_dcm_utms_prisma.sql](sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql).
*   **Prisma**: There are indications of integration with Prisma data, particularly in the `relationships.yaml` file and commented-out sections in SQL scripts like [mart__delivery__join_dcm_utms_prisma.sql](sql/base/dcm/mart__delivery__join_dcm_utms_prisma.sql).
*   **Basis**: DCM data is unified with Basis data to provide a comprehensive view of delivery across different platforms, as demonstrated in [mart__delivery__unified_dcm_basis.sql.sql](sql/marts/delivery/mart__delivery__unified_dcm_basis.sql.sql) and [mart__mft.sql](sql/marts/mft/mart__mft.sql).
*   **`stg__basis__utms.sql`** [stg__basis__utms.sql](sql/base/basis/stg__basis__utms.sql): This file mentions standardizing creative names to "DCM Creative" and references `basis_utms_fromDCM`, indicating a flow of UTM data originating from DCM.

---

## Research Query

track dependancies in /Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/base/basis/stg2__basis__plus_utms.sql

*Session: c456293527e1aa449faa09c756da6963 | Generated: 7/24/2025, 4:02:26 PM*

### Analysis Summary

# Codebase Dependency Report

## High-Level Overview
This report details the dependencies of the SQL file [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql). This file is responsible for joining Basis delivery data with parsed UTM parameters, creating several views for different use cases (with and without supplementary UTMs, and a deduplicated version).

## Mid-Level Dependencies

### [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql)
This SQL script defines several views that combine delivery data with UTM information. It has multiple sections, each creating a different version of the `basis_plus_utms` view.

#### External Data Sources
The script directly queries the following external BigQuery tables/views:
*   **`looker-studio-pro-452620.repo_stg.basis_delivery`**: This table serves as the primary source for delivery data. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:20), [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:54), and [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:100).
*   **`looker-studio-pro-452620.repo_stg.basis_utms_stg_view_2507`**: This view provides parsed UTM data for the first query. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:40).
*   **`looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`**: This table is a source for supplementary UTM data, used in the `utm1` CTE of the second and third queries. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:90) and [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:159).
*   **`looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload`**: This table is another source for UTM data, specifically used in the `utm4` CTE of the third query. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:140).

#### Internal Common Table Expressions (CTEs)
The script utilizes several CTEs to process and join data:
*   **`del`**: This CTE selects all columns from `basis_delivery` and applies initial filtering. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:19), [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:53), and [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:99).
*   **`utm1`**: This CTE processes UTM data from `b_sup_pivt_unioned_tab`, extracting and cleaning various UTM parameters. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:65) and [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:146).
*   **`utm`**: This CTE selects distinct UTM parameters and creates a `utm_utm_key` for joining. It combines data from `utm1` and `utm4` in the third query. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:92) and [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:160).
*   **`utm4`**: This CTE processes UTM data from `dcm_plus_utms_upload`, cleaning creative names and creating a `utm_utm_key`. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:110).
*   **`joined`**: This CTE performs a full join between the `del` and `utm` CTEs based on `del_key` and `utm_utm_key`. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:163).
*   **`ranked`**: This CTE applies a deduplication logic using `ROW_NUMBER()` over the `joined` CTE. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:180).

#### Output Views
The script creates the following views:
*   **`looker-studio-pro-452620.repo_stg.basis_plus_utms_v3`**: This view is created by the first query, joining `del` with `basis_utms_stg_view_2507`. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:4).
*   **`looker-studio-pro-452620.repo_stg.basis_plus_utms_v3_PnS_table`**: This view is created by the second query, joining `del` with `utm` (derived from `utm1`). Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:49).
*   **`looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`**: This view is created by the third query, which includes deduplication logic and combines UTM data from both `utm1` and `utm4`. Cited in [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql:96).

---

## Research Query

Investigate the "b_sup_pivt_unioned_tab" component in detail. Provide a comprehensive analysis of its architecture, responsibilities, and implementation details.

*Session: ed74c3cfa48d0c28296076d9c48b529b | Generated: 7/24/2025, 4:16:31 PM*

### Analysis Summary

# b_sup_pivt_unioned_tab Component Analysis

The **`b_sup_pivt_unioned_tab`** component is a BigQuery table that serves as a crucial source for supplementary UTM (Urchin Tracking Module) data within the codebase. Its primary responsibility is to provide raw, un-processed UTM information that is then integrated and transformed by downstream staging processes.

## Architecture and Responsibilities

The **`b_sup_pivt_unioned_tab`** is a foundational data source, acting as a repository for pivoted and unioned UTM data. It is not directly responsible for data transformation or cleaning but rather for making the raw, supplementary UTM data available for consumption by other SQL scripts.

### External Relationships

This table is primarily consumed by SQL staging scripts, notably [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql) and [stg3_b_plus_utms_PnS.sql](util/basis_utms/stg3_b_plus_utms_PnS.sql). It is referenced by its fully qualified BigQuery path: `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`.

## Implementation Details

The **`b_sup_pivt_unioned_tab`** is directly queried within the `FROM` clause of SQL statements to retrieve its data.

### Integration with `stg2__basis__plus_utms.sql`

In [stg2__basis__plus_utms.sql](sql/base/basis/stg2__basis__plus_utms.sql), the `b_sup_pivt_unioned_tab` is a key input for the `utm1` Common Table Expression (CTE). The `utm1` CTE is responsible for selecting and processing various UTM parameters from this table, including `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, and `utm_term`. It also performs data cleaning and standardization, such as converting `utm_source` to lowercase and handling `NULL` values.

For example, the table is referenced in the `utm1` CTE as seen in [stg2__basis__plus_utms.sql:110](sql/base/basis/stg2__basis__plus_utms.sql:110) and [stg2__basis__plus_utms.sql:245](sql/base/basis/stg2__basis__plus_utms.sql:245).

### Integration with `stg3_b_plus_utms_PnS.sql`

Similarly, the [stg3_b_plus_utms_PnS.sql](util/basis_utms/stg3_b_plus_utms_PnS.sql) script also directly queries `b_sup_pivt_unioned_tab` to retrieve supplementary UTM data, as shown in [stg3_b_plus_utms_PnS.sql:47](util/basis_utms/stg3_b_plus_utms_PnS.sql:47). This indicates its consistent role as a raw data provider for subsequent staging layers.

### Usage in `utm_validation_scrap.sql`

The table is also used in [utm_validation_scrap.sql](util/basis_utms/utm_validation_scrap.sql:43), likely for validation or testing purposes related to UTM data.

