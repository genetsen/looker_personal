# MIQ Polaris Email to V3 MVP

This plan bootstraps MIQ delivery received through the Polaris Email ingestion path into the V3 master data model. MIQ remains the partner for both the new Polaris Email path and the existing FPD path.

## Approved MVP Contract

| Area | Approved behavior |
|---|---|
| Partner | MIQ |
| Preferred path | Polaris Email during each package's successfully loaded minimum-to-maximum date range |
| Fallback path | Existing FPD outside Polaris Email coverage |
| Platforms | Facebook, Instagram, and TikTok |
| Mapping owner | `landing.polaris_email_package_mapping` |
| Delivery owner | `landing.polaris_email_delivery_daily` |
| V3 detail grain | Package, date, platform, campaign, ad group, and ad |
| Audit behavior | FPD landing rows remain physically unchanged; model precedence suppresses their final metrics only inside Polaris Email coverage |
| Automation | Deferred until the manual production run succeeds |

## Delivery Stages

| Stage | Result | Exit proof |
|---|---|---|
| 1. Reader correction | QA comparison treats FPD dates as cumulative snapshots. | Focused tests and the 992-row local review pass. |
| 2. Landing MVP | Five approved source keys and the guarded 992-row delivery snapshot are stored in BigQuery. | Zero invalid, unmapped, duplicate, or ambiguous rows; exact source reconciliation. |
| 3. Model candidate | Compatibility base and V3 candidates apply package-specific Polaris Email precedence. | SQL Change Guard and focused Polaris, FPD, MIQ, manual-edit, planned-carrier, and unrelated-package checks pass. |
| 4. Manual production run | Mapping, delivery, compatibility base, clustered dependency, and V3 are deployed in that order. | Focused live checks match the validated candidate. |
| 5. Small follow-up | The proven manual loader is scheduled without changing its validation contract. | Automation reports a terminal successful load and model refresh. |

## Five Approved Mapping Keys

| Feed | Platform | Campaign | Ad group | Package |
|---|---|---|---|---|
| Meta | Facebook | Awareness Campaign | ACR - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Facebook | Awareness Campaign | Interests - Facebook | Facebook Awareness (`P3HF7QB`) |
| Meta | Instagram | Awareness Campaign | ACR - Instagram | Instagram (`P3HF7T8`) |
| Meta | Instagram | Awareness Campaign | Interests - Instagram | Instagram (`P3HF7T8`) |
| TikTok | TikTok | Purely Elizabeth - Awareness Q3 | Interests | TikTok (`P3HF88Q`) |

## Safety Boundary

The loader requires exactly one current Meta file and one current TikTok file. It stops before production replacement when the source inventory is ambiguous, a source value lacks one approved active mapping, a required value or metric is invalid, or a natural detail key is duplicated. It uploads to a staging table first and replaces the production delivery snapshot only after warehouse-side gates pass.

Manual package edits continue to override source delivery. Planned metrics remain summable on only one natural row per package/date. The existing FPD duplicate-row issue remains outside this MVP because the new path replaces modeled FPD metrics during overlapping coverage without altering the underlying FPD evidence.
