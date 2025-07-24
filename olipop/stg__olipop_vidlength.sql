create or replace view looker-studio-pro-452620.repo_stg.stg__olipop_vidlength as

with 
tt as (
SELECT 
cast (ad_id as string) as ad_id,
ad_name as ad_name,
cast(a.video_id as string) as video_id,
v.duration as duration,
concat(ad_name, "~",v.duration) as video_name
FROM `giant-spoon-299605.tiktok_ads.ad_history` as a left join
giant-spoon-299605.tiktok_ads.video_history as v on 
a.video_id = v.video_id),


fb_1 as (
  select distinct
  a.id,
  a.name, 
  cast(a.creative_id as string) as creative_id,
  c.video_video_id
  from
  giant-spoon-299605.facebook_ads.ad_history a left join
  `giant-spoon-299605.facebook_ads.creative_history` c on cast(a.creative_id as string) = cast(c.id as string)
),

fb as (

select 
cast(fb_1.id as string) as ad_id,
fb_1.name as ad_name,
cast(v.id as string) as video_id, 
v.length as duration,
cast(concat(fb_1.name,"|",v.title,"~",v.length) as string) as video_name,
from fb_1 left join

`giant-spoon-299605.facebook_ads.ad_video_history` v on cast(fb_1.video_video_id as string) = cast(v.id as string)),

yt as (

SELECT 
cast(ad_id as string) as ad_id,
ad_name, 
cast(title as string) as video_id,
ad_duration as duration,
concat(ad_name,"|",ad_format_type,"~", ad_duration) as video_name
FROM `looker-studio-pro-452620.repo_google_ads.google_ads_video_stats_vw`)

select * from fb union distinct 
select * from tt union distinct 
select * from yt