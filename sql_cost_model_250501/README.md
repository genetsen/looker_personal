# Cost Model Comparison SQL Files

This folder contains SQL scripts to compare cost models v4.0 and v4.1 for package ID 'P2R44S2'.

## Files Overview

1. **Original Cost Model Files**
   - `costModel250501_v4.0.bqsql` - Original v4.0 model
   - `costModel_250501_v4.1.bqsql` - Updated v4.1 model with improvements

2. **Package-Specific Test Queries**
   - `costModel250501_v4.0_P2R44S2_test.bqsql` - v4.0 model filtered to P2R44S2
   - `costModel_250501_v4.1_P2R44S2_test.bqsql` - v4.1 model filtered to P2R44S2

3. **Comparison Scripts**
   - **`compare_P2R44S2.bqsql`** - **RECOMMENDED** - Streamlined comparison query that shows differences
   - `costModel_comparison_P2R44S2.bqsql` - Full comparison query (complex)
   - `costModel_comparison_P2R44S2_simplified.bqsql` - Alternative approach (with issues)

## How to Use

To compare the cost differences between v4.0 and v4.1 for package P2R44S2:

1. Run `compare_P2R44S2.bqsql` in BigQuery
2. Results will show:
   - Side-by-side costs from both models
   - Absolute and percentage differences
   - Logic path flags from both models
   - Changes in logic path

## Key Differences Between Models

- **New Metric:** v4.1 introduces `total_inflight_impressions` (only impressions within flight window)
- **CPM Logic:** v4.1 uses in-flight impressions instead of all impressions for overdelivery calculation
- **Flat Fee Logic:** v4.1 improves distribution of costs for flat fee campaigns
- **Prorated Cost:** v4.1 has special handling for live campaigns vs. ended campaigns
- **Flag Calculation:** v4.1 calculates flags earlier in the process

## About the Package P2R44S2

Use this package ID to test the models as it demonstrates the differences in cost calculation logic.
