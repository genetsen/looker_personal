# looker_personal

SQL- and BigQuery-first analytics workspace for campaign reporting pipelines across ADIF, Olipop, MFT, and shared utility workflows.

## Core Workflows

- ADIF TV and digital pipeline (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- ADIF updated FPD integration (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif`)
- Olipop cross-platform delivery + video joins (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/sql/marts/olipop`)
- MFT export and mart views (`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/mft`)

## New Capability: ADIF Social Layer From Cross-Platform Raw

- Source table: `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
- Layer SQL: `/Users/eugenetsenter/Looker_clonedRepo/looker_personal/adif/sql/stg__adif__social_crossplatform.sql`
- Default output view: `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- Current inclusion logic: keep rows where normalized text contains both `adif` and `social`

## Notes

- The ADIF social layer keeps original grain (daily ad-level rows) and all source metrics.
- If you want a mart/table target, materialize from the staging view to your selected dataset.
