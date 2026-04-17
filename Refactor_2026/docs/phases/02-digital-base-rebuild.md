# Phase 02: Digital Base Rebuild

- Status: `pending`
- Main plan: [PLAN.md](../../PLAN.md)

## Purpose

Rebuild the digital base first.

This phase should separate:

- reusable digital source preparation
- ADIF-specific digital stitching rules

## Inputs

- shared FPD source model
- shared Prisma source model
- shared delivery source model
- current ADIF digital rules that must be preserved

## Outputs

- rebuilt shared digital input models
- rebuilt ADIF digital base model

## Logic To Preserve

- current client filters
- current package remaps
- current join behavior
- current source-priority behavior for the digital base

## Success Gate

The rebuilt digital base matches the current digital behavior closely enough to trust before moving to Phase 03.
