# Basis UTM regression diagnosis — July 20, 2026

## What changed

- A replacement of the shared Basis UTM enrichment view added an FY26 audio creative-name fallback.
- The replacement was deployed at 16:48 UTC and the final MFT refresh succeeded afterward.

## What the user's exact export proves

- Before: 117 distinct FY26 Q2/Q3 placement/creative rows with blank `utm_content`.
- Current: 135 distinct rows with blank `utm_content`.
- All 117 prior rows remain; 18 CTV rows were added; none were removed from the missing set.
- The 18 added CTV rows do not have an exact key in the current Basis/DCM UTM candidate sources.

## Current boundary

- No production writes were made after the user reported the regression.
- The intended audio-only change altered the shared view and changed CTV output outside the requested scope.
- The exact prior live view definition is not stored in the repository; a rollback must restore the prior definition or otherwise reproduce the pre-change 117-row result before refreshing again.
