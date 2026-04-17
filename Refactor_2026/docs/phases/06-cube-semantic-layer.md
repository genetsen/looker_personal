# Phase 06: Cube Semantic Layer

- Status: `pending`
- Main plan: [PLAN.md](../../PLAN.md)

## Purpose

Add the Cube semantic layer only after the rebuilt ADIF marts and compatibility output are trusted.

## Inputs

- trusted rebuilt ADIF marts
- trusted compatibility output

## Outputs

- Cube semantic definitions on top of trusted rebuilt outputs

## Logic To Preserve

- semantic definitions should reflect trusted rebuilt data, not invent new business logic

## Success Gate

The Cube layer points only at trusted rebuilt outputs and matches the approved ADIF business meaning.
