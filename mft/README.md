# MFT View Pipeline

Complete documentation of the MFT (MassMutual Full-funnel Tracking) data pipeline that unifies DCM and Basis programmatic advertising data with UTM parameter enrichment.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              BASE DATA SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  DCM SOURCES:                           BASIS SOURCES:                          │
│  • DCM.20250505_costModel_v5            • giant-spoon-299605.basis_master2      │
│    (129,733 rows)                         (132,915 rows)                        │
│                                                                                 │
│  UTM SOURCES:                                                                   │
│  • mm_utms_snapshot (11,373 rows)       • b_sup_pivt_unioned_tab (1,616 rows)   │
│  • dcm_plus_utms_upload (97 rows)                                               │
└─────────────────┬───────────────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            STAGING LAYER (Views)                                 │
├─────────────────────────────────┬───────────────────────────────────────────────┤
│   DCM BRANCH                    │         BASIS BRANCH                          │
│                                 │                                               │
│ final_views.dcm ───────────┐    │ repo_stg.basis_delivery                       │
│ final_views.utms_view ─────┼──► │      │                                        │
│                            │    │      ▼                                        │
│        ┌───────────────────┘    │ basis_plus_utms_v4_PnS_table                  │
│        ▼                        │ (joins delivery + UTM sources)                │
│  repo_stg.dcm_plus_utms         │                                               │
│  (LEFT JOIN on ad_name)         │                                               │
└────────────────┬────────────────┴────────────────┬───────────────────────────────┘
                 │                                  │
                 └──────────────┬───────────────────┘
                                ▼
                  ┌─────────────────────────────┐
                  │     MART LAYER (Final)      │
                  │     repo_mart.mft_view      │
                  │   (UNION ALL + Filters)     │
                  │       134,283 rows          │
                  └─────────────────────────────┘
```

## Table of Contents

- [Data Sources](#data-sources)
- [Staging Layer](#staging-layer)
- [Mart Layer](#mart-layer)
- [UTM Processing Scripts](#utm-processing-scripts)
- [Key Concepts](#key-concepts)
- [Current State](#current-state)
- [Usage Examples](#usage-examples)

---

## Data Sources

### DCM Data

#### DCM Cost Model
**Table**: `looker-studio-pro-452620.DCM.20250505_costModel_v5`

| Attribute | Value |
|-----------|-------|
| **Rows** | 129,733 |
| **Purpose** | DoubleClick Campaign Manager delivery data with cost modeling |
| **Update Frequency** | Daily |
| **Shared With** | ADIF pipeline |

**Key Fields**:
- `date` - Delivery date
- `campaign` - Campaign name
- `package_roadblock` - Package/roadblock identifier
- `placement_id` - Unique placement identifier
- `ad` - Ad name (used for UTM join)
- `creative` - Creative name
- `impressions` - Ad impressions delivered
- `clicks` - Click-through events
- `media_cost` - Raw media cost
- `daily_recalculated_cost` - Normalized daily cost (used in final output)

---

### Basis Data

#### Basis Master (Delivery)
**Table**: `giant-spoon-299605.data_model_2025.basis_master2`

| Attribute | Value |
|-----------|-------|
| **Rows** | 132,915 |
| **Purpose** | Programmatic ad delivery from Basis DSP |
| **Update Frequency** | Daily |
| **Last Updated** | Jan 12, 2026 (12:01) |

**Key Fields**:
- `date` - Delivery date
- `campaign` - Campaign name
- `package_roadblock` - Package identifier
- `tactic` - Media tactic
- `placement` - Placement name (includes CP_XXXXX ID)
- `creative_name` - Creative asset name
- `impressions` - Ad impressions
- `clicks` - Click events
- `media_cost` - Spend

**Note**: Also accessible via `looker-studio-pro-452620.landing.basis_master` (160,638 rows - includes additional campaigns)

---

### UTM Metadata Sources

#### 1. MM UTMs Snapshot (Primary DCM UTMs)
**Table**: `giant-spoon-299605.data_model_2025.mm_utms_snapshot`

| Attribute | Value |
|-----------|-------|
| **Rows** | 11,373 |
| **Purpose** | Primary UTM reference for DCM placements |
| **Last Updated** | Jan 12, 2026 (12:04) |

**Key Fields**:
- `Campaign` - Campaign name
- `Site_Name` - Publisher/site
- `Package_Name` - Package identifier
- `Placement_Name` - Placement name
- `Ad_Name` - Ad name (join key to DCM)
- `_UTM_Source` - Traffic source tag
- `_UTM_Medium` - Marketing medium tag
- `_UTM_Campaign` - Campaign tag
- `_UTM_Content` - Content variation tag
- `_UTM_Term` - Search term tag

---

#### 2. Basis UTM Pivot Table
**Table**: `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`

| Attribute | Value |
|-----------|-------|
| **Rows** | 1,616 |
| **Purpose** | UTM mappings extracted from Basis trafficking sheets |
| **Source** | Excel trafficking sheets via R script |

**Key Fields**:
- `tag_placement` - Placement tag name
- `name` - Creative name
- `url` - Full URL with UTM parameters
- Extracted UTM parameters via regex

**URL Parsing Logic**:
```sql
REGEXP_EXTRACT(url, 'utm_source=(.*?)&')  AS utm_source,
REGEXP_EXTRACT(url, 'utm_medium=(.*?)&')  AS utm_medium,
REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
REGEXP_EXTRACT(url, 'utm_term=(.*)')      AS utm_term,
REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content
```

---

#### 3. DCM Plus UTMs Upload (Manual Corrections)
**Table**: `looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload`

| Attribute | Value |
|-----------|-------|
| **Rows** | 97 |
| **Purpose** | Manual UTM corrections and additions |
| **Update** | As needed |

Used for edge cases where automated matching fails.

---

## Staging Layer

### DCM Branch

#### View: `final_views.dcm`
**Definition**: Simple pass-through from cost model
```sql
SELECT * FROM looker-studio-pro-452620.DCM.20250505_costModel_v5
```

#### View: `final_views.utms_view`
**Definition**: Filtered UTM snapshot
```sql
SELECT * FROM `giant-spoon-299605.data_model_2025.mm_utms_snapshot`
WHERE _utm_source <> "#VALUE!"
```

#### View: `repo_stg.dcm_plus_utms`
**Purpose**: Enriches DCM delivery data with UTM parameters

**Full SQL**:
```sql
WITH joined AS (
  SELECT
    dcm.date,
    dcm.campaign,
    dcm.package_roadblock,
    dcm.package_id,
    dcm.placement_id,
    dcm.impressions,
    dcm.KEY,
    dcm.ad,
    dcm.click_rate,
    dcm.clicks,
    dcm.creative,
    dcm.media_cost,
    dcm.rich_media_video_completions,
    dcm.rich_media_video_plays,
    dcm.total_conversions,
    -- ... (additional DCM planning fields)
    dcm.daily_recalculated_cost,
    dcm.daily_recalculated_imps,

    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,

    /* UTM enrichment */
    utm._UTM_Source    AS utm_source,
    utm.Ad_Name        AS ad_name,
    utm.Placement_Name AS placement_name,
    utm._UTM_Campaign  AS utm_campaign,
    utm._UTM_Medium    AS utm_medium,
    utm._UTM_Content   AS utm_content,
    utm._UTM_Term      AS utm_term
  FROM `looker-studio-pro-452620.final_views.dcm` AS dcm
  LEFT JOIN `looker-studio-pro-452620.final_views.utms_view` AS utm
    ON dcm.ad = utm.ad_name
),

/* Flag exact duplicates */
ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined)  -- identical across ALL columns
      ORDER BY placement_id                 -- arbitrary tie-breaker
    ) AS rn
  FROM joined
)

/* Deliver deduped result */
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1
```

**Key Features**:
- LEFT JOIN preserves all DCM records (even without UTM match)
- `TO_JSON_STRING` deduplication catches exact row duplicates
- Retains all cost model planning/delivery fields

---

### Basis Branch

#### View: `repo_stg.basis_delivery`
**Location**: Defined in `sql/base/basis/stg__basis__delivery.sql`
**Purpose**: Adds join helper fields to basis_master2

**Key Transformations**:

1. **Placement ID Extraction**:
```sql
REGEXP_EXTRACT(placement, r'CP_(\d+)') AS id
```

2. **Creative Name Normalization** (for UTM matching):
```sql
LOWER(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      LOWER(
        REPLACE(
          REGEXP_EXTRACT(
            creative_name,
            r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'  -- strip prefix & size
          ),
          ' ', ''  -- remove spaces
        )
      ),
      r'(^peacock_|_peacock$)', ''  -- drop "peacock"
    ),
    r'[^a-zA-Z0-9]', ''  -- keep only alphanumeric
  )
) AS cleaned_creative_name
```

3. **Composite Join Key**:
```sql
CONCAT(
  LOWER(placement),
  " || ",
  cleaned_creative_name
) AS del_key
```

**Example Transformation**:
| Input | Output |
|-------|--------|
| `123_PEACOCK_Stay Ready_300x250_v2` | `stayready` |
| `CP_45678_MassMutual_Display` | `massmutual_display` → key: `cp_45678_massmutual_display \|\| stayready` |

---

#### View: `repo_stg.basis_plus_utms_v4_PnS_table`
**Purpose**: Joins Basis delivery with UTM parameters from multiple sources

**Full SQL Structure**:
```sql
WITH
-- 1. Delivery data (filtered)
del AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
),

-- 2. UTM source from dcm_plus_utms_upload
utm4 AS (
  SELECT
    placement,
    -- Normalized creative name (same regex as basis_delivery)
    REGEXP_REPLACE(...) AS cleaned_creative_name_2,
    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
    CONCAT(LOWER(placement), " || ", cleaned_creative_name_2) AS utm_utm_key
  FROM looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload
),

-- 3. UTM source from trafficking sheets
utm1 AS (
  SELECT
    tag_placement AS placement,
    name AS creative_name,
    -- Extract UTMs from URL
    REGEXP_EXTRACT(url, 'utm_source=(.*?)&') AS utm_source,
    REGEXP_EXTRACT(url, 'utm_medium=(.*?)&') AS utm_medium,
    REGEXP_EXTRACT(url, 'utm_campaign=(.*?)&') AS utm_campaign,
    REGEXP_EXTRACT(url, 'utm_term=(.*)') AS utm_term,
    REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content,
    -- Normalized creative + composite key
    ...
  FROM `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`
),

-- 4. Combined UTM reference (deduplicated)
utm AS (
  SELECT DISTINCT
    placement, cleaned_creative_name_2,
    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
    CONCAT(LOWER(placement), " || ", cleaned_creative_name_2) AS utm_utm_key
  FROM utm1
  UNION DISTINCT
  SELECT * FROM utm4
),

-- 5. Full join delivery + UTMs
joined AS (
  SELECT
    del.*,
    utm.placement AS placement__utms,
    utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    utm.utm_utm_key AS utm_key,
    COALESCE(del.del_key, utm_utm_key) AS master_key
  FROM del
  FULL JOIN utm ON del_key = utm.utm_utm_key
  WHERE campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
),

-- 6. Deduplicate by date + master_key
ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY date, master_key
      ORDER BY placement
    ) AS rn
  FROM joined
)

SELECT * EXCEPT(rn, meta_data_date_pull, package, gmail_dt, meta_data_date_range)
FROM ranked
WHERE rn = 1
ORDER BY date DESC NULLS FIRST
```

**Key Features**:
- FULL JOIN captures UTM-only records (for validation)
- UNION DISTINCT combines multiple UTM sources
- Per-day deduplication ensures unique records
- Campaign filtering removes non-MassMutual data

---

## Mart Layer

### Final Output: `repo_mart.mft_view`

**Location**: `looker-studio-pro-452620.repo_mart.mft_view`

| Attribute | Value |
|-----------|-------|
| **Total Rows** | 134,283 |
| **Date Range** | 2025-01-01 to 2026-01-10 |
| **Primary Use** | UTM-enriched delivery reporting |

**Full SQL**:
```sql
-- DCM records (MassMutual campaigns)
SELECT
  `date`,
  `campaign`,
  `package_roadblock`,
  `placement_name`,
  `utm_source`,
  `utm_medium`,
  `utm_campaign`,
  `utm_content`,
  `utm_term`,
  `daily_recalculated_cost` AS cost,
  `impressions`,
  `clicks`,
  creative
FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms`
WHERE date >= DATE '2025-01-01'
  AND package_roadblock LIKE '%MASS%'
  AND impressions > 10

UNION ALL

-- Basis records (programmatic)
SELECT
  `date`,
  `campaign`,
  NULL AS `package_roadblock`,
  CONCAT(placement, " -- ", creative_name) AS placement_name,
  `utm_source`,
  `utm_medium`,
  `utm_campaign`,
  `utm_content`,
  `utm_term`,
  `media_cost` AS cost,
  `impressions`,
  `clicks`,
  cleaned_creative_name AS creative
FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`
WHERE date >= DATE '2025-01-01'
  AND impressions > 10
```

---

### Output Schema

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `date` | DATE | Both | Delivery date |
| `campaign` | STRING | Both | Campaign name |
| `package_roadblock` | STRING | DCM only | Package/roadblock identifier (NULL for Basis) |
| `placement_name` | STRING | Both | Placement identifier (formatted differently per source) |
| `utm_source` | STRING | Both | Traffic source (e.g., "basis", "mindbodygreen") |
| `utm_medium` | STRING | Both | Marketing medium (e.g., "ott", "standard", "display") |
| `utm_campaign` | STRING | Both | Campaign tag for analytics tracking |
| `utm_content` | STRING | Both | Content variation identifier |
| `utm_term` | STRING | Both | Search/targeting term |
| `cost` | FLOAT | Both | Daily spend (`daily_recalculated_cost` for DCM, `media_cost` for Basis) |
| `impressions` | INTEGER | Both | Ad impressions (filtered: > 10) |
| `clicks` | INTEGER | Both | Click-through events |
| `creative` | STRING | Both | Creative name (normalized for Basis) |

---

## UTM Processing Scripts

### R Script: `util/util__basis__utm_pivot_longer.r`

**Purpose**: Processes Basis trafficking sheets to extract UTM parameters

#### Input Files
```r
file_path1 <- 'Flight 1_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
file_path2 <- 'Flight 2_Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'
file_path3 <- 'Flight 3 Trafficking Sheet_MASSMUTUAL003CP_DSP.xlsx'

sheet_name1 <- "Q1 Tsheet"
sheet_name2 <- "Flight 2_Updated"
sheet_name3 <- "Flight 3"
```

#### Processing Steps

**1. Auto-detect Header Row**
```r
# Read first 20 rows to find header
temp_df <- read_excel(file_path, sheet = sheet_name, n_max = 20, col_names = FALSE)

# Find row containing "Property" in first column
header_row <- which(temp_df[[1]] == "Property")
skip_rows <- header_row[1] - 1
```

**2. Read and Clean Data**
```r
new_df <- read_excel(file_path, sheet = sheet_name, skip = skip_rows) %>%
  clean_names()
```

**3. Normalize Column Names**

Trafficking sheets have wide format with multiple creatives:
```
creative_1_name, creative_1_url, creative_2_name, creative_2_url, ...
```

Script normalizes to:
```
creative_1_name, creative_1_url, creative_2_name, creative_2_url, ...
```

**4. Pivot to Long Format**

Converts wide creative columns to rows for easier processing.

**5. Extract UTM Parameters**

Uses regex to parse URLs and extract individual UTM values.

**6. Upload to BigQuery**

Final processed data uploaded to `utm_scrap.b_sup_pivt_unioned_tab`.

---

### Related Files

| File | Purpose |
|------|---------|
| `util/util__basis__utm_pivot_longer.r` | Main UTM extraction script |
| `util/util__basis__utm_pivot_longer_clean.r` | Cleaned version |
| `util/util__basis__utm_pivot_longer_loop.r` | Batch processing version |
| `util/basis_utms/union_basis_utms.ipynb` | Jupyter notebook for UTM unioning |
| `util/basis_utms/b_utms_diagram.md` | Pipeline diagram (outdated) |
| `sql/base/basis/stg__basis__delivery.sql` | Basis delivery staging view |
| `sql/base/basis/stg__basis__utms.sql` | Basis UTM staging |
| `sql/base/basis/stg2__basis__plus_utms.sql` | Combined Basis + UTMs |

---

## Key Concepts

### 1. Creative Name Normalization

**Problem**: Same creative has different names across systems.

| System | Example Name |
|--------|--------------|
| DCM | `MassMutual_StayReady_300x250_v2` |
| Basis | `123_PEACOCK_Stay Ready_300x250` |
| UTM Sheet | `stay-ready-creative` |

**Solution**: Multi-step regex normalization:

```sql
-- Step-by-step transformation
'123_PEACOCK_Stay Ready_300x250_v2'
  → '123_PEACOCK_Stay Ready'           -- Strip size suffix
  → 'peacock_stay ready'               -- Lowercase
  → 'stay ready'                       -- Remove "peacock"
  → 'stayready'                        -- Remove spaces
  → 'stayready'                        -- Keep alphanumeric only
```

**Impact**: Increases UTM match rate from ~40% to ~85%.

---

### 2. Composite Join Keys

**Structure**:
```
del_key = LOWER(placement) || " || " || cleaned_creative_name
```

**Example**:
```
cp_45678_massmutual_display || stayreadybrandv2
```

**Why**: Single creative can run on multiple placements. Composite key ensures unique matching.

---

### 3. Multi-Source UTM Strategy

**Priority Order**:
1. `mm_utms_snapshot` - Primary source (most complete)
2. `b_sup_pivt_unioned_tab` - Trafficking sheet extracts
3. `dcm_plus_utms_upload` - Manual corrections

**Combination Logic**:
```sql
utm AS (
  SELECT DISTINCT ... FROM utm1    -- Trafficking sheets
  UNION DISTINCT
  SELECT * FROM utm4               -- Manual uploads
)
```

---

### 4. Deduplication Strategies

**DCM Branch**: Full row deduplication
```sql
ROW_NUMBER() OVER (
  PARTITION BY TO_JSON_STRING(joined)  -- Hash all columns
  ORDER BY placement_id
)
```

**Basis Branch**: Date + Key deduplication
```sql
ROW_NUMBER() OVER (
  PARTITION BY date, master_key
  ORDER BY placement
)
```

---

### 5. Minimum Impression Threshold

**Filter**: `impressions > 10`

**Rationale**:
- Removes test impressions
- Filters data noise
- Reduces row count ~15%
- Improves query performance

---

## Current State

### Data Distribution

#### By Source
| Source | Records | Percentage |
|--------|---------|------------|
| Basis | 82,710 | 61.6% |
| DCM | 51,573 | 38.4% |
| **Total** | **134,283** | 100% |

#### By UTM Source
| utm_source | utm_medium | Records | % of Total |
|------------|------------|---------|------------|
| basis | ott | 50,474 | 37.6% |
| basis | audio | 11,604 | 8.6% |
| mindbodygreen | standard | 11,127 | 8.3% |
| basis | ctv | 8,776 | 6.5% |
| basis | display | 7,349 | 5.5% |
| investmentnews | standard | 3,922 | 2.9% |
| thenewyorktimes | standard | 3,285 | 2.4% |
| wallstreetjournal | standard | 3,179 | 2.4% |
| advisorperspectives | standard | 2,889 | 2.2% |
| financialtimes | standard | 2,441 | 1.8% |

#### By Campaign
| Campaign | Description |
|----------|-------------|
| MassMutualWealthManagement2025 | Wealth management focus |
| MassMutualLVGP2025 | LVGP initiative |
| Massachusetts Mutual Full Funnel Branding FY25 | Brand awareness |
| MassMutual20252026Media | General media |
| MassMutualStayReady2025 | Stay Ready campaign |
| MassMutualVolatility2025 | Market volatility messaging |

---

### Update Schedule

| Component | Frequency | Last Updated |
|-----------|-----------|--------------|
| DCM Cost Model | Daily | Jan 9, 2026 |
| Basis Master | Daily | Jan 12, 2026 |
| MM UTMs Snapshot | Daily | Jan 12, 2026 |
| UTM Pivot Table | As needed | Jan 12, 2026 |

---

## Usage Examples

### Query 1: Daily Performance by Channel
```sql
SELECT
  date,
  utm_medium AS channel,
  SUM(cost) AS total_spend,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr_pct
FROM `looker-studio-pro-452620.repo_mart.mft_view`
WHERE date >= '2025-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### Query 2: Publisher Performance
```sql
SELECT
  utm_source AS publisher,
  COUNT(DISTINCT date) AS active_days,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(cost) AS total_spend,
  SAFE_DIVIDE(SUM(cost), SUM(impressions)) * 1000 AS cpm
FROM `looker-studio-pro-452620.repo_mart.mft_view`
WHERE utm_medium = 'standard'  -- Direct publisher buys
GROUP BY 1
HAVING total_impressions > 10000
ORDER BY total_spend DESC
```

### Query 3: Creative Performance
```sql
SELECT
  creative,
  campaign,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr_pct,
  SUM(cost) AS spend
FROM `looker-studio-pro-452620.repo_mart.mft_view`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1, 2
HAVING impressions > 1000
ORDER BY ctr_pct DESC
LIMIT 20
```

### Query 4: UTM Match Rate Analysis
```sql
SELECT
  CASE
    WHEN utm_source IS NULL THEN 'No UTM Match'
    ELSE 'UTM Matched'
  END AS match_status,
  COUNT(*) AS records,
  SUM(impressions) AS impressions,
  SUM(cost) AS spend
FROM `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1
```

### Query 5: Source Comparison (DCM vs Basis)
```sql
SELECT
  CASE
    WHEN package_roadblock IS NOT NULL THEN 'DCM'
    ELSE 'Basis'
  END AS data_source,
  COUNT(*) AS records,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(cost) AS spend,
  SAFE_DIVIDE(SUM(cost), SUM(impressions)) * 1000 AS avg_cpm
FROM `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1
```

---

## Comparison with ADIF Pipeline

| Aspect | MFT View | ADIF Pipeline |
|--------|----------|---------------|
| **Primary Purpose** | UTM-enriched delivery tracking | Planned vs actual reconciliation |
| **Client Focus** | MassMutual | Forevermark US |
| **Shared Source** | DCM.20250505_costModel_v5 | DCM.20250505_costModel_v5 |
| **Unique Sources** | Basis DSP, UTM sheets | FPD, Prisma, TV estimates |
| **Join Strategy** | LEFT JOIN (preserve delivery) | FULL OUTER JOIN (capture gaps) |
| **Key Enrichment** | UTM parameters | First-party data, planned budgets |
| **Output Columns** | 13 | 18 |
| **Row Count** | 134,283 | 12,085 |

---

## Maintenance

### Daily Checks
1. Verify row counts haven't dropped unexpectedly
2. Check for new campaigns requiring UTM mapping
3. Monitor UTM match rates

### Weekly Tasks
1. Review unmatched UTM records
2. Update trafficking sheet extracts if new flights launched
3. Validate creative name normalization catching new patterns

### Monthly Tasks
1. Archive old UTM upload files
2. Review and optimize view performance
3. Update documentation for schema changes

---

## Troubleshooting

### Issue: Low UTM Match Rate

**Symptoms**: Many NULL values in utm_source/utm_medium

**Diagnosis**:
```sql
SELECT
  COUNT(*) AS total,
  COUNTIF(utm_source IS NULL) AS no_utm,
  COUNTIF(utm_source IS NULL) / COUNT(*) * 100 AS pct_unmatched
FROM `looker-studio-pro-452620.repo_mart.mft_view`
```

**Solutions**:
1. Check if new placements need UTM mapping
2. Verify creative name normalization regex
3. Add manual mappings to `dcm_plus_utms_upload`

---

### Issue: Duplicate Records

**Symptoms**: Same date/placement appearing multiple times

**Diagnosis**:
```sql
SELECT date, placement_name, creative, COUNT(*) AS dupes
FROM `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
ORDER BY dupes DESC
```

**Solutions**:
1. Check deduplication logic in staging views
2. Verify join keys are unique
3. Review source data for duplicates

---

## Contact

For questions or issues with this pipeline, contact the data engineering team.

**Related Documentation**:
- [ADIF Pipeline README](../adif/README.md) - Sister pipeline for Forevermark
- [Basis UTMs Diagram](../util/basis_utms/b_utms_diagram.md) - Legacy diagram
- [DCM Cost Model](../sql/base/dcm/) - Shared DCM processing

**Last Updated**: January 12, 2026
