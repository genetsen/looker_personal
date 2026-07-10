# Manual Edit Restore List - 2026 Non-Social Non-Amazon

Purpose: restore candidates for older Manual Data Editor values that are preserved in the July 8 field-delta audit but absent from current active manual raw and daily BigQuery state.

Safety boundary: this file does not write to Google Sheets or BigQuery. It is a review/restore checklist built from saved evidence plus a read-only live BigQuery presence check.

## Counts

| Metric | Value |
| --- | ---: |
| Restore field records | 159 |
| Package IDs | 49 |
| Current active raw rows for these package IDs | 0 |
| Current active daily rows for these package IDs | 0 |

## Changed Field Counts

| Field | Records |
| --- | ---: |
| Impressions | 47 |
| Clicks | 34 |
| Spend | 33 |
| Video Plays | 12 |
| Video Completions | 12 |
| Delivery Override Start Date | 8 |
| Delivery Override End Date | 8 |
| Planned Spend | 2 |
| Planned Impressions | 1 |
| Flight Start Date | 1 |
| Flight End Date | 1 |

## Advertiser Counts

| Advertiser | Records |
| --- | ---: |
| MassMutual | 98 |
| A Diamond Is Forever | 36 |
| Olipop | 15 |
| Ritual | 5 |
| Apollo | 5 |

## Top Restore Candidates

| Rank | Package Friendly Name | Field | Manual Value To Restore | Baseline Without Manual | Delta |
| ---: | --- | --- | ---: | ---: | ---: |
| 1 | MIQ_P3GT8NK_260601\|260831_OLIPOPLTOBrandPlatform2026_Spotify_CPM_pRate:19_p$:500000 | Impressions | 483,676 | 10,156,778 | -9,673,102 |
| 2 | MIQ_P3FFSQL_260401\|260630_ADIF2026_Q2PremiumCTV_CPM_pRate:34_p$:1080200 | Impressions | 25,559,237 | 33,854,913 | -8,295,676 |
| 3 | MIQ_P3GT8FP_260601\|260831_OLIPOPLTOBrandPlatform2026_PremiumCTVPubList_CPM_pRate:34_p$:1085000 | Video Plays | 5,028,237 | 12,784,626 | -7,756,389 |
| 4 | MIQ_P3GT8FP_260601\|260831_OLIPOPLTOBrandPlatform2026_PremiumCTVPubList_CPM_pRate:34_p$:1085000 | Impressions | 5,054,664 | 12,803,137 | -7,748,473 |
| 5 | MIQ_P3GT8FP_260601\|260831_OLIPOPLTOBrandPlatform2026_PremiumCTVPubList_CPM_pRate:34_p$:1085000 | Video Completions | 4,945,255 | 12,584,431 | -7,639,176 |
| 6 | MIQ_P3FHBC6_260501\|260531_ADIF2026_Q2BridalDOOH_CPM_pRate:27_p$:0 | Planned Impressions | 3,703,704 | 0 | 3,703,704 |
| 7 | MIQ_P3FFV6H_260401\|260630_ADIF2026_Q2HighImpactDisplay_CPM_pRate:17_p$:125000 | Impressions | 6,383,369 | 9,303,144 | -2,919,775 |
| 8 | PLAYFY_P3FFPRW_260330\|260930_20252026Media_VideoCommercialMLB_Free_pRate:0_p$:0 | Impressions | 15,581,482 | 18,243,509 | -2,662,027 |
| 9 | PLAYFY_P3FFPRW_260330\|260930_20252026Media_VideoCommercialMLB_Free_pRate:0_p$:0 | Video Plays | 14,024,395 | 16,400,125 | -2,375,730 |
| 10 | MIQ_P3GXB31_260601\|260831_OLIPOPLTOBrandPlatform2026_AVPremiumCTVPubList_Free_pRate:0_p$:0 | Impressions | 1,377,108 | 3,570,815 | -2,193,707 |
| 11 | PLAYFY_P3FFPRW_260330\|260930_20252026Media_VideoCommercialMLB_Free_pRate:0_p$:0 | Video Completions | 12,729,781 | 14,815,942 | -2,086,161 |
| 12 | DISNED_P38F8WB_251101\|260930_20252026Media_VideoCommercial_CPM_pRate:32.45_p$:1479999.97 | Impressions | 36,205,250 | 37,901,350 | -1,696,100 |
| 13 | DISNED_P38F8WB_251101\|260930_20252026Media_VideoCommercial_CPM_pRate:32.45_p$:1479999.97 | Video Plays | 36,159,474 | 37,853,675 | -1,694,201 |
| 14 | DISNED_P38F8WB_251101\|260930_20252026Media_VideoCommercial_CPM_pRate:32.45_p$:1479999.97 | Video Completions | 35,771,088 | 37,443,146 | -1,672,058 |
| 15 | MIQ_P3GXB31_260601\|260831_OLIPOPLTOBrandPlatform2026_AVPremiumCTVPubList_Free_pRate:0_p$:0 | Video Plays | 1,341,520 | 2,729,668 | -1,388,148 |
| 16 | MIQ_P3GXB31_260601\|260831_OLIPOPLTOBrandPlatform2026_AVPremiumCTVPubList_Free_pRate:0_p$:0 | Video Completions | 1,319,927 | 2,690,767 | -1,370,840 |
| 17 | CONDE_P3FDP4F_260601\|260630_ADIF2026_Taylor&TravisWeddingSponsorship_Flat_pRate:150000_p$:150000 | Impressions | 0 | 1,352,811 | -1,352,811 |
| 18 | MIQ_P3FFSVK_260401\|260630_ADIF2026_Q2CTVLiveSports_CPM_pRate:70_p$:450000 | Impressions | 6,022,352 | 7,215,692 | -1,193,340 |
| 19 | DISCOV_P3FNGKL_260601\|260630_20252026Media_ROSInFront_CPM_pRate:85.8_p$:120000 | Impressions | 711,116 | 1,794,270 | -1,083,154 |
| 20 | DISNED_P3FQX94_260526\|260630_20252026Media_KERVLBar_CPM_pRate:39.85_p$:175711 | Impressions | 2,829,366 | 3,735,952 | -906,586 |
| 21 | ESPN_P3GL55Z_260506\|260628_20252026Media_LiabilityWipe_Flat_pRate:0_p$:0 | Impressions | 1,867,362 | 2,560,052 | -692,690 |
| 22 | ESPN_P3GL55Z_260506\|260628_20252026Media_LiabilityWipe_Flat_pRate:0_p$:0 | Video Plays | 1,863,218 | 2,554,601 | -691,383 |
| 23 | ESPN_P3GL55Z_260506\|260628_20252026Media_LiabilityWipe_Flat_pRate:0_p$:0 | Video Completions | 1,693,451 | 2,345,130 | -651,679 |
| 24 | DISCOV_P3FNGKL_260601\|260630_20252026Media_ROSInFront_CPM_pRate:85.8_p$:120000 | Video Plays | 232,946 | 831,667 | -598,721 |
| 25 | DISCOV_P3FNGKL_260601\|260630_20252026Media_ROSInFront_CPM_pRate:85.8_p$:120000 | Video Completions | 227,686 | 811,731 | -584,045 |

Full field-level restore list: `2026-07-09-manual-edit-restore-list-2026-non-social-non-amazon.csv`.
