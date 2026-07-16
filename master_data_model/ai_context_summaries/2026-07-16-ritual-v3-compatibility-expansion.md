# Ritual v3 compatibility expansion context

## Goal

Update the live BigQuery compatibility view used by the Ritual Omni topic so it both reads from `master_stg.data_model_v3` and exposes every v3 field that the older compatibility projection currently hides.

## Confirmed live state

- Omni dashboard topic: `master_stg__ritual_data_model`.
- BigQuery compatibility view: `looker-studio-pro-452620.master_stg.ritual_data_model`.
- The view now reads from `looker-studio-pro-452620.master_stg.data_model_v3` and still filters to Ritual.
- The existing compatibility projection exposes the older field contract but omits 115 v3 source columns.
- Existing Omni queries still run after the source switch.

## Required correction

- Preserve all existing compatibility field names and semantics.
- Add the 115 currently hidden v3 fields under their canonical v3 names.
- Prefer a projection that also carries future new v3 columns automatically without duplicating already-mapped source fields.
- Build and validate an isolated QA candidate before replacing the live view.
- Use SQL Change Guard for baseline-versus-candidate row, metric, key-coverage, and schema proof.
- After deployment, run a focused live BigQuery check and an Omni query that uses at least one newly exposed field.
- Update the active SQL, README, model map, local changelog, and parent changelog; exclude unrelated dirty files from the commit.

## Omni branch

An unmerged Omni native branch named `ritual-v3-compatibility-2026-07-16` exists with branch ID `5bb6ab1a-09d4-4aa9-950f-57e0a5c7acee`. It contains the source-switch version of `master_stg/ritual_data_model.view` and will need the expanded live SQL after BigQuery validation.
