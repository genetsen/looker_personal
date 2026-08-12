# Changelog

## 2026-08-12

**Ritual Gmail fallback now follows the runner's approved OAuth client** ([Python](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/DCM_API/dcm_api/gmail_reports.py)) — 🟢 **Verified and committed**<br>The [Ritual Gmail loader workflow](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/DCM_API/README.md#ritual-gmail-loader-auth) now tries the same approved Gmail OAuth client the Universal Runner uses before falling back to the older Python pickle token, and the updated path completed a live scheduled-attachment download plus BigQuery load without changing the downstream table target. [More details](/Users/eugenetsenter/.codex/automations/load-ritual-qtd-reach-to-bigquery/memory.md)

### Pending Next Actions

- **Since Aug 12** - Decide whether to retire the legacy Python pickle fallback after more successful G2-based runs - RECOMMENDED
