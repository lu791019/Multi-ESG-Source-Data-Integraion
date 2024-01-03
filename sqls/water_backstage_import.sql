delete from app.water_backstage_update
where period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant not in ('WZS-1','WZS-3','WZS-6','WZS-8','WKS-1','WKS-5','WKS-6');




with
ranking_tmp as
(
select
*
from raw.wks_opm_ui_finparam wmfd
where site = 'WKS'
and "period" >= to_char(date_trunc('month', now()) - INTERVAL '1 month','YYYY-mm')
and "period" < to_char(date_trunc('month', now()),'YYYY-mm')
and plant not in ('F237','F230')
and plant in (select plant_code from raw.plant_mapping pm where site ='WKS')
union
select
*
from raw.wks_opm_ui_finparam wmfd
where site = 'WZS'
and "period" >= to_char(date_trunc('month', now()) - INTERVAL '1 month','YYYY-mm')
and "period" < to_char(date_trunc('month', now()),'YYYY-mm')
and plant not in ('F139','F138','F130','F136')
and plant in (select plant_code from raw.plant_mapping pm where site ='WZS')
)
,


plant_total_tmp as (
select site,plant,"period",(sum(dlnum)+sum(idlnum)) as plant_total
from ranking_tmp
group by site,plant,"period"
)
,

--select * from plant_total_tmp


plant_total as (
select * from plant_total_tmp
where period = (SELECT MAX(period) FROM plant_total_tmp )
and plant_total >0
)
,

--select * from plant_total


all_total_tmp as (
select
site
,"period"
,sum(plant_total) as "all_total"
from plant_total_tmp
group by site,"period"
)
,


all_total as (
select * from all_total_tmp
where period = (SELECT MAX(period) FROM all_total_tmp )
)
,

--select * from all_total


temp_rate as (
select
--*,
ptt.period as "period"
,plant,
p.bo ,
ptt.site,
p.plant_name,
(plant_total/all_total) as "ratio"
from plant_total ptt
left join all_total att
on ptt.site = att.site
and ptt."period" = att."period"
left join raw.plant_mapping p
on ptt.plant = p.plant_code
where ptt.plant not in ('生活區','Others')
)
,



temp_dsrb as (
select
plant_name,
cwi.period_start,
cwi.indicatoryear,
cwi.indicatorm,
cwi.indicatorvalue*trt.ratio as "indicatorvalue"
from temp_rate trt
left join raw.csr_water_indicator cwi
on trt.site = cwi.sitename
where cwi.sitename not in ('WZS')
)
,




final_table as (
select
tdrwks.plant_name as "plant",
Date(period_start) as "period_start",
indicatoryear,
indicatorm,
indicatorvalue,
NOW() as "last_update_time"
from temp_dsrb tdrwks
left join temp_rate trwks
on tdrwks.plant_name = trwks.plant
union
select
--cwi.sitename as "plant",
 case when cwi.sitename in ('WIHK1') then 'WIHK-1'
 when cwi.sitename in ('WIHK2') then 'WIHK-2'
 when cwi.sitename in ('WMYP1') then 'WMY-1'
 when cwi.sitename in ('WMYP2') then 'WMY-2'
 when cwi.sitename in ('WCD') then 'WCD-1'
 when cwi.sitename in ('WVN') then 'WVN-1'
 else cwi.sitename end as "plant",
Date(period_start),
indicatoryear,
indicatorm,
indicatorvalue,
NOW() as "last_update_time"
from raw.csr_water_indicator cwi
where cwi.sitename not in ('WKS','WZS')
)


insert into app.water_backstage_update
select * from final_table
where period_start >= date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and indicatorvalue !=0
and plant not in ('WZS-1','WZS-3','WZS-6','WZS-8','WKS-1','WKS-5','WKS-6');




