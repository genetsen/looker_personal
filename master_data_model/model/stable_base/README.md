# Stable Base

Stable package/date base model and upstream support refreshes.

The current final model still reads this base, so it remains active support. It is not the preferred starting point for current final-table questions unless the issue is about base lineage or upstream refresh behavior.

## Social source boundary

The base model retains the physical social API feed for audit, but excludes a social daily row before it enters the modeled package/date output when all of the following are true:

- the package has a known pacing end date;
- the source row is later than that end date; and
- spend, impressions, clicks, and video metrics are all literally zero.

This prevents zero-value post-flight API tail rows from creating partial date pairs that downstream package lookups could combine incorrectly. Rows with nonzero delivery or unknown metric values remain available for investigation.
