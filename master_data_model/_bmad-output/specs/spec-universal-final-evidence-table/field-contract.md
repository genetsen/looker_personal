# Field Contract

This companion defines the readable final table contract. It is self-teaching for downstream agents: a grain is what one row represents, and additive means a metric can be safely summed across rows.

## Row Identity Fields

| Field | Required | Meaning |
|---|---:|---|
| `univ_source_system` | Yes | Original source family, such as DCM, FPD, TV, social, Amazon, Prisma, or manual. |
| `univ_source_row_id` | Yes | Stable source-row identifier or generated trace key. |
| `univ_row_grain` | Yes | What one row represents, such as package/date, creative/date, DMA/date, or package/creative/DMA/date. |
| `univ_source_lineage` | Yes | Human-readable source path or upstream object label. |
| `univ_record_date` | Yes | Delivery or evidence date used for time grouping. |

## Universal Field Prefix

| Rule | Decision | Reason |
|---|---|---|
| New universal fields need a prefix. | Use `univ_`. | The current master table already has legacy, QA, and source-specific fields. A prefix makes new universal fields easy to recognize. |
| Legacy fields keep their current names. | Do not rename existing master-table columns just to fit the new prefix. | Legacy column parity requires old fields to remain available as-is. |

## Placeholder Convention

| Placeholder | Use When | Plain-English Meaning |
|---|---|---|
| `not_available_at_source` | The source does not provide that dimension at all. | The box exists in the final table, but this source has no label to put in it. |
| `unknown_from_source` | The source has the field, but the value is blank, unknown, or unusable. | The source should know this value, but it did not give a usable one. |
| `not_applicable_to_source` | The dimension does not make business sense for the source. | This label does not apply to this kind of row. |

## Dimension Slots

| Dimension Slot | If Source Has It | If Source Does Not Have It |
|---|---|---|
| `package_id` | Use actual package ID. | Use `not_available_at_source`. |
| `package_name` | Use actual, selected, or approved inferred package name. | Use `not_available_at_source` or `unknown_from_source`, depending on the source contract. |
| `placement_id` | Use actual placement ID. | Use `not_available_at_source`. |
| `ad_id_or_name` | Use actual ad identifier or name. | Use `not_available_at_source`. |
| `creative_id_or_name` | Use actual creative identifier or name. | Use `not_available_at_source`. |
| `dma` | Use actual DMA or market. | Use `not_available_at_source`. |
| `campaign` | Use actual or approved inferred campaign. | Use `not_available_at_source` or `unknown_from_source`, depending on the source contract. |
| `client_or_advertiser` | Use actual or approved inferred client or advertiser. | Use `not_available_at_source` or `unknown_from_source`, depending on the source contract. |
| `supplier_or_site` | Use actual supplier, site, publisher, or partner. | Use `not_available_at_source` or `unknown_from_source`, depending on the source contract. |

## Metadata Priority

| Priority | Metadata Source | Rule |
|---:|---|---|
| 1 | Actual source metadata | Keep when present and trusted. |
| 2 | Approved manual metadata | Use where the manual workflow validates it. |
| 3 | High-confidence inferred metadata | Use only where actual and manual values are missing. |
| 4 | Lower-confidence inferred metadata | Preserve for visibility unless approved for final selection. |
| 5 | Placeholder | Use when no actual, manual, or acceptable inferred value exists. |

## Inferred Metadata Rules

| Final Field Need | Actual Present | Actual Missing |
|---|---|---|
| Flight start | Keep actual/source start date. | Use minimum delivery date. |
| Flight end | Keep actual/source end date. | Use maximum delivery date. |
| Planned spend | Keep actual/source planned spend. | Use total observed delivery spend. |
| Planned impressions | Keep actual/source planned impressions. | Use total observed delivery impressions. |

## Inference Audit Fields

| Field | Meaning |
|---|---|
| `metadata_status` | actual, manual, inferred, mixed, or placeholder. |
| `flight_start_source` | actual, manual, or inferred_from_min_delivery_date. |
| `flight_end_source` | actual, manual, or inferred_from_max_delivery_date. |
| `planned_spend_source` | actual, manual, or inferred_from_total_delivery_spend. |
| `planned_impressions_source` | actual, manual, or inferred_from_total_delivery_impressions. |
| `metadata_inference_reason` | Short reason explaining the inference. |
| `metadata_inference_evidence` | The source clue used for inference, such as a package ID in an ad name. |
| `metadata_confidence` | high, medium, low, or not_applicable. |

## Metric Status And Summability

Every number needs two labels: where it came from, and whether it can be added across rows.

| Label | Example Values | Plain-English Meaning |
|---|---|---|
| Metric value status | `direct`, `inferred`, `allocated`, `unavailable` | Tells the user whether the number came straight from source, was filled from evidence, was allocated, or is not present. |
| Metric summability | `additive`, `doNotSum`, `blocked` | Tells the user whether the number can be safely summed, should only be viewed as context, or should not be exposed for rollups. |

## Metric Safety

| Metric Type | Example Fields | Value Status | Summability |
|---|---|---|---|
| Row-level delivery metrics | `spend`, `impressions`, `clicks`, `video_views` | `direct` | `additive` |
| Row-level allocated metrics | `allocated_spend`, `allocated_impressions` | `allocated` | `additive`, when allocation grain is declared |
| Inferred plan-like fallback | selected planned spend or planned impressions filled from delivered totals | `inferred` | Usually `doNotSum` unless allocated to the row grain |
| Repeated package context | `package_total_spend_doNotSum`, `package_total_impressions_doNotSum` | `direct` or `inferred` | `doNotSum` |
| Source raw context | Source total fields repeated on detail rows | `direct` | `doNotSum` unless explicitly converted into row-level allocation |
| Missing metric | Any metric the source cannot provide | `unavailable` | `blocked` |

## Missing Value Rule

Missing values must be explicit enough for a beginner analyst to understand. Nulls may exist in raw source fields, but selected final dimension fields should prefer standardized placeholders so grouping does not hide records.
