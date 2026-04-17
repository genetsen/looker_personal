# Phase 03: Updated FPD Overlay

- Status: `pending`
- Main plan: [PLAN.md](../../PLAN.md)

## Purpose

Rebuild the updated-FPD branch after the digital base is trusted.

## Inputs

- rebuilt digital base from Phase 02
- current updated-FPD source and date-spreading logic

## Outputs

- rebuilt updated-FPD daily model
- rebuilt ADIF overlay model

## Logic To Preserve

- current package-total spreading across Prisma dates
- current overlay behavior
- current updated-FPD priority over original FPD and DCM where applicable

## Success Gate

The rebuilt updated-FPD branch matches the current overlay behavior closely enough to trust before moving to Phase 04.
