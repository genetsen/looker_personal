-- Create the current per-feed source checkpoint used by the Polaris Email
-- loader. The guarded seed inserts only when the exact verified source object
-- is still represented in production landing; it never overwrites later state.

CREATE TABLE IF NOT EXISTS
  `looker-studio-pro-452620.landing.polaris_email_source_state` (
    client_id STRING NOT NULL,
    connection_id STRING NOT NULL,
    source_feed STRING NOT NULL,
    source_object_uri STRING NOT NULL,
    source_object_generation STRING NOT NULL,
    object_created_at TIMESTAMP NOT NULL,
    source_max_date DATE NOT NULL,
    successful_load_at TIMESTAMP NOT NULL
  )
CLUSTER BY client_id, connection_id, source_feed
OPTIONS (
  description = 'Current successful Cloud Storage object checkpoint for each Polaris Email feed. The loader updates this table in the same transaction as the affected landing feed.'
);

MERGE `looker-studio-pro-452620.landing.polaris_email_source_state` AS target
USING (
  SELECT *
  FROM UNNEST([
    STRUCT(
      'C70545844' AS client_id,
      '11694' AS connection_id,
      'meta' AS source_feed,
      'gs://bkt-plrs-prd-data-imports-7nqd/data/client_id=C70545844/connection_id=11694/purely-elizabeth-daily-report-08-18-meta/2026_08_20_1787245251682_0.csv' AS source_object_uri,
      '1787245252374637' AS source_object_generation,
      TIMESTAMP '2026-08-20 17:00:52.391+00' AS object_created_at,
      DATE '2026-08-17' AS source_max_date,
      TIMESTAMP '2026-08-20 20:33:06+00' AS successful_load_at
    ),
    STRUCT(
      'C70545844',
      '11694',
      'tiktok',
      'gs://bkt-plrs-prd-data-imports-7nqd/data/client_id=C70545844/connection_id=11694/purely-elizabeth-daily-report-08-18-tiktok/2026_08_20_1787245227897_0.csv',
      '1787245228232875',
      TIMESTAMP '2026-08-20 17:00:28.238+00',
      DATE '2026-08-17',
      TIMESTAMP '2026-08-20 20:33:06+00'
    )
  ])
) AS seed
ON target.client_id = seed.client_id
  AND target.connection_id = seed.connection_id
  AND target.source_feed = seed.source_feed
WHEN NOT MATCHED AND EXISTS (
  SELECT 1
  FROM `looker-studio-pro-452620.landing.polaris_email_delivery_daily` AS production
  WHERE production.client_id = seed.client_id
    AND production.connection_id = seed.connection_id
    AND production.source_feed = seed.source_feed
    AND production.source_object_uri = seed.source_object_uri
)
THEN INSERT (
  client_id,
  connection_id,
  source_feed,
  source_object_uri,
  source_object_generation,
  object_created_at,
  source_max_date,
  successful_load_at
)
VALUES (
  seed.client_id,
  seed.connection_id,
  seed.source_feed,
  seed.source_object_uri,
  seed.source_object_generation,
  seed.object_created_at,
  seed.source_max_date,
  seed.successful_load_at
);
