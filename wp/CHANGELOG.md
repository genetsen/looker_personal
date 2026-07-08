# Changelog

## 2026-07-07

- **FIXED** - Excluded `1000heads` campaigns in the shared-social production builder before they can enter shared-social staging or flow into the master data model.

### Pending Next Actions

- **Since Jul 7** - Update the saved `stg__olipop__crossplatform_raw_tbl_sched` transfer config with owner-account credentials so future scheduled runs keep excluding `1000heads` campaigns
