-- Purpose: keep the live Basis delivery view aligned with the Basis UTM lookup
-- for FY26 CTV creative names that include both a 16x9 token and a 0x0 suffix.
-- Reads: landing.basis_master. Replaces: repo_stg.basis_delivery.
-- Safety: the alternate key is limited to the FY26 campaign, CTV placements,
-- and the Autograph and Play by Play families that had no existing UTM match.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.basis_delivery` AS
WITH base_delivery AS (
  SELECT
    *,
    REGEXP_EXTRACT(placement, r'CP_(\d+)') AS id,
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(
            REPLACE(
              REGEXP_EXTRACT(
                creative_name,
                r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'
              ),
              ' ',
              ''
            )
          ),
          r'(^peacock_|_peacock$)',
          ''
        ),
        r'[^a-zA-Z0-9]',
        ''
      )
    ) AS cleaned_creative_name,
    IFNULL(
      CONCAT(
        LOWER(placement),
        ' || ',
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              LOWER(
                REPLACE(
                  REGEXP_EXTRACT(
                    creative_name,
                    r'^(?:\d+_)?([^_]+.*?)(?:_\d+x\d+.*)?$'
                  ),
                  ' ',
                  ''
                )
              ),
              r'(^peacock_|_peacock$)',
              ''
            ),
            r'[^a-zA-Z0-9]',
            ''
          )
        )
      ),
      CONCAT(placement, ' || ', creative_name)
    ) AS del_key
  FROM `looker-studio-pro-452620.landing.basis_master`
)
SELECT
  * REPLACE (
    CASE
      WHEN campaign = 'Massachusetts Mutual Connected Funnel FY26'
        AND REGEXP_CONTAINS(LOWER(placement), r'_ctv_')
        AND REGEXP_CONTAINS(cleaned_creative_name, r'^(autograph|playbyplay)')
      THEN CONCAT(
        LOWER(placement),
        ' || ',
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER(
              REGEXP_REPLACE(
                REGEXP_REPLACE(creative_name, r'0x0', ''),
                r'(?:_?\d+x\d+.*)?$',
                ''
              )
            ),
            r'[^A-Za-z0-9\s]',
            ''
          ),
          r'\s+',
          ''
        )
      )
      ELSE del_key
    END AS del_key
  )
FROM base_delivery;
