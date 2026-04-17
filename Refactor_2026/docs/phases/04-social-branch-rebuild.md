# Phase 04: Social Branch Rebuild

- Status: `pending`
- Main plan: [PLAN.md](../../PLAN.md)

## Purpose

Rebuild the social branch after the digital base and updated-FPD branch are trusted.

## Inputs

- rebuilt upstream digital outputs
- current ADIF social mapping logic
- current pacing inputs used by the ADIF social branch

## Outputs

- rebuilt ADIF social mapping models
- rebuilt ADIF social append model

## Logic To Preserve

- current ADIF social filtering
- current mapping from ad set to package and ad to placement
- current pacing enrichment behavior

## Success Gate

The rebuilt social branch matches the current social behavior closely enough to trust before moving to Phase 05.
