# Architecture Diagrams

Diagrams live in a companion so the SPEC kernel stays prose-only.

## Universal Final Evidence Flow

```mermaid
flowchart TD
  A["Raw source rows<br/>DCM, FPD, TV, Social, Amazon, Prisma, Manual"] --> B["Clean source adapters<br/>standard field names"]
  M["Agent-guided source mapper<br/>HTML mapping widget"] --> B
  M --> N["Mapping configuration<br/>versioned and validated"]
  N --> B
  B --> C["dbt staging models<br/>natural useful source grain"]
  C --> D["Inferred metadata creation<br/>fill blanks only"]
  D --> E["Metric and allocation rules<br/>additive vs doNotSum"]
  E --> F["Universal final evidence table<br/>readable at multiple grains"]
  F --> G["Package summary shortcut"]
  F --> H["Creative summary shortcut"]
  F --> I["DMA summary shortcut"]
  F --> J["Dashboard mart shortcut"]
```

## dbt And BigQuery Zone Flow

```mermaid
flowchart TD
  A["mdm_raw or source-owned raw<br/>source receipts"] --> B["mdm_stg<br/>clean source models"]
  C["mdm_config<br/>source mapper outputs"] --> B
  B --> D["mdm_int<br/>inference and metric safety"]
  D --> G["mdm_qa<br/>diffs and validation proof"]
  D --> E["mdm_mart<br/>universal evidence and marts"]
  E --> F["mdm_publish<br/>stable BI views"]
  E --> H["mdm_sandbox<br/>temporary prototypes"]
```

## Metadata Selection Flow

```mermaid
flowchart TD
  A["Need final metadata field"] --> B{"Actual source value exists?"}
  B -- "Yes" --> C["Use actual source value"]
  B -- "No" --> D{"Approved manual value exists?"}
  D -- "Yes" --> E["Use manual value"]
  D -- "No" --> F{"High-confidence inference exists?"}
  F -- "Yes" --> G["Use inferred value and mark source"]
  F -- "No" --> H["Use placeholder and preserve issue label"]
```

## Metric Safety Flow

```mermaid
flowchart TD
  A["Metric enters final table"] --> B["Label value status<br/>direct, inferred, allocated, unavailable"]
  B --> C{"Is metric row-level or allocated to row grain?"}
  C -- "Yes" --> D["Label summability as additive"]
  C -- "No" --> E["Label summability as doNotSum or blocked"]
  D --> F["Allowed in approved rollups"]
  E --> G["Visible for context or blocked from normal summing"]
```
