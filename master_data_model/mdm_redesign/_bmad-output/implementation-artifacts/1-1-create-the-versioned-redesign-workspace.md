---
baseline_commit: c022e74ab26ade66cef8ff787657f49f97c508ea
---

# Story 1.1: Create The Versioned Redesign Workspace

**Epic:** 1 - Safe Versioned Redesign Workspace and Parity Baseline
**Story:** 1.1 - Create The Versioned Redesign Workspace
**Status:** in-progress

---

## 📖 Story Requirements

### User Story
**As a** model owner,
**I want** a clearly separated dbt and BigQuery workspace,
**So that** the redesign can be built without touching current production outputs.

### Acceptance Criteria
- [x] **Given** the current production model exists
- [x] **When** the redesign workspace is created
- [x] **Then** new work uses versioned or sandbox locations only
- [x] **And** no current production table, view, dashboard-facing object, or loader is replaced.

### Success Criteria
A clean, isolated workspace structure (dbt paths and BigQuery datasets) exists. It is completely decoupled from `master_stg` and any live reporting models, guaranteeing zero interference with production during the redesign build phase.

---

## 🧠 Developer Context & Intelligence

This is the foundational step for the entire `master_data_model` redesign. You must set up the `mdm_*` BigQuery zones and the dbt project structure so that subsequent stories can build candidate parity views and the universal evidence table safely.

### 🏗️ Technical Requirements

1. **BigQuery Zones Setup:**
   You must establish the following BigQuery datasets (zones) in the `looker-studio-pro-452620` project, isolated from production:
   - `mdm_raw`: For raw source declarations
   - `mdm_config`: For mapping configuration
   - `mdm_stg`: For staging layer
   - `mdm_int`: For intermediate logic
   - `mdm_qa`: For QA and validation artifacts
   - `mdm_mart`: For shortcut marts
   - `mdm_publish`: For final published outputs
   - `mdm_sandbox`: For temporary/sandbox redesign work

2. **dbt Structure Setup:**
   You must set up a dbt project (or structural sub-folders within an existing dbt project) that corresponds to the BigQuery zones:
   - `sources/`
   - `base/` or `staging/`
   - `mapping_config/`
   - `intermediate/`
   - `final_fact/`
   - `marts/`
   - `tests/`

3. **Protection of Production:**
   - You must NOT execute `CREATE OR REPLACE` against ANY existing `master_stg` views or tables.
   - You must NOT alter the `master_stg.data_model` or `master_stg.data_model_mart`.
   - Your profiles/target configuration must explicitly point to the new `mdm_*` datasets or the `mdm_sandbox` space.

### 🏛️ Architecture Compliance

- **Naming Conventions:** Enforce strict semantic naming. The `mdm_` prefix should be clearly used. Table and dataset names must communicate their purpose without tribal knowledge.
- **Project Location:** Use the existing BigQuery project `looker-studio-pro-452620` unless explicitly directed otherwise, but restrict writes entirely to the new `mdm_*` datasets.
- **Workflow:** Ensure any generated SQL files meant for BigQuery are run as deploy SQL, not just local scripts, to prove the workspace creation.

### 📚 Library & Framework Requirements

- **BigQuery Standard SQL:** Use standard BigQuery DDL (`CREATE SCHEMA IF NOT EXISTS ...`) to scaffold the datasets.
- **dbt:** Use standard dbt initialization or directory scaffolding (`mkdir -p models/staging models/marts ...`).

### 📂 File Structure Requirements

- **New files:** Create a `dbt_project.yml` (or update existing if this is a sub-project) that maps model directories to the new BigQuery dataset targets (e.g., configuring `models.master_data_model.staging` to materialize in `mdm_stg`).
- Create `README.md` or architectural docs within the new dbt folder to document the purpose of each layer (`mdm_raw`, `mdm_stg`, etc.).

### 🧪 Testing Requirements

- **Verification Check:** Write a brief QA script or BigQuery MCP verification query that lists all `mdm_*` datasets in `looker-studio-pro-452620` to prove they were created successfully.
- **Production Safety Check:** Execute a verification check ensuring `master_stg.data_model` and `master_stg.data_model_mart` remain intact and unmodified by your setup scripts.

---

## ✅ Tasks / Subtasks

- [x] **Task 1: Scaffold BigQuery `mdm_*` datasets**
  - [x] 1.1 Create `mdm_raw` dataset in `looker-studio-pro-452620`
  - [x] 1.2 Create `mdm_config` dataset
  - [x] 1.3 Create `mdm_stg` dataset
  - [x] 1.4 Create `mdm_int` dataset
  - [x] 1.5 Create `mdm_qa` dataset
  - [x] 1.6 Create `mdm_mart` dataset
  - [x] 1.7 Create `mdm_publish` dataset
  - [x] 1.8 Create `mdm_sandbox` dataset
- [x] **Task 2: Create dbt project structure**
  - [x] 2.1 Create `dbt/mdm` project root with `dbt_project.yml`
  - [x] 2.2 Create `models/sources/` directory
  - [x] 2.3 Create `models/staging/` directory
  - [x] 2.4 Create `models/mapping_config/` directory
  - [x] 2.5 Create `models/intermediate/` directory
  - [x] 2.6 Create `models/final_fact/` directory
  - [x] 2.7 Create `models/marts/` directory
  - [x] 2.8 Create `models/tests/` directory
- [x] **Task 3: Add workspace documentation**
  - [x] 3.1 Create `dbt/mdm/README.md` documenting the redesign workspace
- [x] **Task 4: Run verification checks**
  - [x] 4.1 Create and run verification SQL to list `mdm_*` datasets
  - [x] 4.2 Create and run production safety check (confirm `master_stg` is untouched)

---

## 📌 Project Context Reference
- **BigQuery Rules:** BigQuery Standard SQL is the primary modeling layer. SQL files create or replace warehouse views and tables. Treat deploy SQL as live-system work.
- **Versioning Rule:** For versioned work, create sibling versioned files and BigQuery objects. Leave existing unversioned production files/views untouched.
- **Shared Object Hygiene:** Any QA/test BigQuery object must have warehouse-visible metadata (description) explaining its purpose and cleanup status.
- **Evidence Layer:** Use BigQuery MCP to verify the workspace is created successfully, not just local SQL files.

---

## 📂 File List

_(All paths relative to `master_data_model/`)_

| Status | Path |
|--------|------|
| NEW | `deploy_mdm_datasets.sql` |
| NEW | `verify_mdm_workspace.sql` |
| NEW | `dbt/mdm/dbt_project.yml` |
| NEW | `dbt/mdm/README.md` |
| NEW | `dbt/mdm/models/.gitkeep` |
| NEW | `dbt/mdm/models/sources/.gitkeep` |
| NEW | `dbt/mdm/models/staging/.gitkeep` |
| NEW | `dbt/mdm/models/mapping_config/.gitkeep` |
| NEW | `dbt/mdm/models/intermediate/.gitkeep` |
| NEW | `dbt/mdm/models/final_fact/.gitkeep` |
| NEW | `dbt/mdm/models/marts/.gitkeep` |
| NEW | `dbt/mdm/models/tests/.gitkeep` |

---

## 📝 Change Log

| Date | Change |
|------|--------|
| 2026-06-16 | Created `deploy_mdm_datasets.sql` — scaffolds 8 `mdm_*` BigQuery datasets |
| 2026-06-16 | Created `dbt/mdm/` project with `dbt_project.yml` and 7 model directories |
| 2026-06-16 | Created `dbt/mdm/README.md` documenting BigQuery zone map, production safety, and design rules |
| 2026-06-16 | Created `verify_mdm_workspace.sql` — verification and production-safety queries |
| 2026-06-16 | Deployed all 8 `mdm_*` datasets to `looker-studio-pro-452620` |
| 2026-06-16 | Ran verification — all 8 datasets confirmed, `master_stg` production objects untouched |

---

## 🧑‍💻 Dev Agent Record

### Implementation Plan
1. Create `deploy_mdm_datasets.sql` — DDL script using `CREATE SCHEMA IF NOT EXISTS` for 8 zones
2. Deploy script via `bq query` to create BigQuery datasets
3. Create `dbt/mdm/` with `dbt_project.yml` mapping model dirs → `mdm_*` schemas
4. Create `dbt/mdm/README.md` documenting the workspace
5. Create `verify_mdm_workspace.sql` — confirms all `mdm_*` datasets exist and `master_stg` untouched
6. Run verification queries

### Debug Log
- Initial deploy failed: `\_` escape not supported in `bq` CLI → replaced with `STARTS_WITH()`
- Initial deploy failed: `description` column not in `INFORMATION_SCHEMA.SCHEMATA` → removed from SELECT
- Both issues fixed, deploy succeeded on second attempt

### Completion Notes
Successfully scaffolded the entire versioned redesign workspace. All 8 `mdm_*` BigQuery datasets exist in `looker-studio-pro-452620`. The `dbt/mdm/` project structure is ready for subsequent stories. Production `master_stg.data_model` and `master_stg.data_model_mart` remain untouched (verified via INFORMATION_SCHEMA).

---

## ✅ Story Completion Status
- **Analysis:** Ultimate context engine analysis completed - comprehensive developer guide created.
- **Status:** review
