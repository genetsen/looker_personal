create or replace table `looker-studio-pro-452620.landing.basis_utms_unioned` as


with 

f1 as (
SELECT 
  line_item,
  tag_placement,
  name,
  end_date,
  start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight1_2`
where name is not null
),

f2 as (
SELECT 
  line_item,
  tag_placement,
  name,
  end_date,
  start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight2_2`
where name is not null
),

f3 as (
SELECT 
  line_item,
  tag_placement,
  name,
  cast(end_date as date) as end_date,
  cast(start_date as date) as start_date,
  size,
  formats,
  url

FROM `looker-studio-pro-452620.landing.basis_utms_pivoted_flight3_2`
where name is not null
),
final as( 
select * from f1 
union all select * from f2
union all select * from f3) 

select * from final 
--where name like "%x%"