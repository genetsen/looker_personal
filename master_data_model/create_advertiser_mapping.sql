-- Create the editable advertiser standardization map used by the master model.
--
-- Reads:
--   - No upstream tables. The rows below are the approved advertiser aliases.
-- Produces:
--   - master_stg.advertiser_mapping.
--
-- Safe modification:
--   Add or update exact aliases here, keep one active row per match field/value,
--   then validate the master-model candidate before replacing production.

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.master_stg.advertiser_mapping`
OPTIONS (
  description = 'Advertiser alias mapping used by master_stg.data_model. Exact source short names and advertiser names resolve to one standardized advertiser label. Unmapped Prisma clients fall back to the cleaned Prisma advertiser name. Owner: master_data_model.'
) AS
SELECT
  match_field,
  UPPER(TRIM(match_value)) AS normalized_match_value,
  prisma_advertiser_name,
  standardized_advertiser_name,
  TRUE AS is_active,
  mapping_note,
  CURRENT_TIMESTAMP() AS updated_at
FROM UNNEST([
  STRUCT('advertiser_short_name' AS match_field, 'ADSK' AS match_value, 'Autodesk, Inc' AS prisma_advertiser_name, 'Autodesk' AS standardized_advertiser_name, 'Approved canonical advertiser.' AS mapping_note),
  STRUCT('advertiser_short_name', 'APO', 'Apollo Global Management', 'Apollo', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'FMUS', 'Forevermark US', 'A Diamond Is Forever', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'GEA', 'GE AEROSPACE', 'GE Aerospace', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'ICE', 'Intercontinental Exchange', 'ICE', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'ITR', 'Cumberland Packing Corp', 'Cumberland Packing', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'MASS', 'MassMutual', 'MassMutual', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'NBC', 'NBC Entertainment', 'NBC Entertainment', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'OLI', 'Olipop, Inc', 'Olipop', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'PURE', 'Purely Elizabeth, LLC', 'Purely Elizabeth', 'Approved canonical advertiser without legal suffix.'),
  STRUCT('advertiser_short_name', 'RTL', 'Ritual', 'Ritual', 'Approved canonical advertiser.'),
  STRUCT('advertiser_short_name', 'SYNC', 'Synchrony', 'Synchrony', 'Approved canonical advertiser.'),
  STRUCT('advertiser_name', 'ADIF USA', 'Forevermark US', 'A Diamond Is Forever', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Forevermark US', 'Forevermark US', 'A Diamond Is Forever', 'Prisma source name.'),
  STRUCT('advertiser_name', 'A Diamond is Forever - US', 'Forevermark US', 'A Diamond Is Forever', 'Existing source alias.'),
  STRUCT('advertiser_name', 'FMUS', 'Forevermark US', 'A Diamond Is Forever', 'Existing source alias.'),
  STRUCT('advertiser_name', 'A Diamond is Forever', 'Forevermark US', 'A Diamond Is Forever', 'Existing source alias.'),
  STRUCT('advertiser_name', 'De Beers Group', 'Forevermark US', 'A Diamond Is Forever', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Apollo Global Management', 'Apollo Global Management', 'Apollo', 'Prisma source name.'),
  STRUCT('advertiser_name', 'Apollo - Giant Spoon', 'Apollo Global Management', 'Apollo', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Apollo Global Management, Inc', 'Apollo Global Management', 'Apollo', 'Existing source alias.'),
  STRUCT('advertiser_name', 'APO', 'Apollo Global Management', 'Apollo', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Autodesk, Inc', 'Autodesk, Inc', 'Autodesk', 'Prisma source name.'),
  STRUCT('advertiser_name', 'ADSK', 'Autodesk, Inc', 'Autodesk', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Cumberland Packing Corp', 'Cumberland Packing Corp', 'Cumberland Packing', 'Prisma source name.'),
  STRUCT('advertiser_name', 'GE AEROSPACE', 'GE AEROSPACE', 'GE Aerospace', 'Prisma source name.'),
  STRUCT('advertiser_name', 'GEA', 'GE AEROSPACE', 'GE Aerospace', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Highlights', 'Highlights', 'Highlights', 'Existing non-Prisma advertiser.'),
  STRUCT('advertiser_name', 'Intercontinental Exchange', 'Intercontinental Exchange', 'ICE', 'Prisma source name.'),
  STRUCT('advertiser_name', 'ICE', 'Intercontinental Exchange', 'ICE', 'Existing source alias.'),
  STRUCT('advertiser_name', 'MassMutual', 'MassMutual', 'MassMutual', 'Prisma source name.'),
  STRUCT('advertiser_name', 'MassMutual_Brand', 'MassMutual', 'MassMutual', 'Existing source alias.'),
  STRUCT('advertiser_name', 'MASS', 'MassMutual', 'MassMutual', 'Existing source alias.'),
  STRUCT('advertiser_name', 'NBC Entertainment', 'NBC Entertainment', 'NBC Entertainment', 'Prisma source name.'),
  STRUCT('advertiser_name', 'Olipop, Inc', 'Olipop, Inc', 'Olipop', 'Prisma source name.'),
  STRUCT('advertiser_name', 'OLIPOP AD ACCOUNT', 'Olipop, Inc', 'Olipop', 'Existing source alias.'),
  STRUCT('advertiser_name', 'OLIPOP INC.', 'Olipop, Inc', 'Olipop', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Olipop', 'Olipop, Inc', 'Olipop', 'Existing source alias.'),
  STRUCT('advertiser_name', 'OLI', 'Olipop, Inc', 'Olipop', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Purely Elizabeth, LLC', 'Purely Elizabeth, LLC', 'Purely Elizabeth', 'Prisma source name without legal suffix.'),
  STRUCT('advertiser_name', 'Ritual', 'Ritual', 'Ritual', 'Prisma source name.'),
  STRUCT('advertiser_name', 'RTL', 'Ritual', 'Ritual', 'Existing source alias.'),
  STRUCT('advertiser_name', 'Synchrony', 'Synchrony', 'Synchrony', 'Prisma source name.')
]);
