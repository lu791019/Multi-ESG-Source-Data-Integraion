delete from app.electricity_backstage_update
where period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant not in ('WZS-1','WZS-3','WZS-6','WZS-8','WKS-1','WKS-5','WKS-6');

with
ranking_tmp as
(
select * ,
row_number() over (order by batch_id desc ) sn
from raw.wks_mfg_fem_dailypower wmfd
where site = 'WKS'
and datadate >= to_char(date_trunc('month', now()) - INTERVAL '2 month','YYYY-mm')
and datadate < to_char(date_trunc('month', now()) - INTERVAL '1 month','YYYY-mm')
and batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower )
and consumetype ='用電量'
and plant in (select plant_code from raw.plant_mapping pm where site ='WKS')
union
select * ,
row_number() over (order by batch_id desc ) sn
from raw.wks_mfg_fem_dailypower wmfd
where site = 'WZS'
and datadate >= to_char(date_trunc('month', now()) - INTERVAL '2 month','YYYY-mm')
and datadate < to_char(date_trunc('month', now()) - INTERVAL '1 month','YYYY-mm')
and batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower )
and consumetype ='用電量'
and plant in (select plant_code from raw.plant_mapping pm where site ='WZS')
)
,

amount_plt as (
select site,plant,consumetype , sum(power) as plt_sum
from ranking_tmp
group by site,plant,consumetype
)
,



total as (
select site,consumetype ,sum(power) as total_sum
from ranking_tmp
group by site,consumetype
)
,



temp_rate as (
select
plant,
p.bo,
plt.site,
case when p.plant_name is null then plant else p.plant_name end as "plant_name",
plt.consumetype, (plt_sum/total_sum) as "ratio"
from amount_plt plt
left join total t
on plt.site = t.site
left join raw.plant_mapping p
on plt.plant = p.plant_code
where plt.plant not in ('生活區','Others')
)
,




temp_dsrb as (
select
plant,
plant_name,
cei.period_start,
cei.indicatoryear,
cei.indicatorm,
cei.indicatorvalue*trt.ratio as "indicatorvalue"
from temp_rate trt
left join raw.csr_electricity_indicator cei
on trt.site = cei.sitename
)
,


final_table as (
select
trwks.plant_name as "plant",
Date(period_start),
indicatoryear,
indicatorm,
indicatorvalue,
NOW() as "last_update_time"
from temp_dsrb tdrwks
left join temp_rate trwks
on tdrwks.plant = trwks.plant
union
select
--cei.sitename as "plant",
case when cei.sitename in ('WMYP1') then 'WMY-1'
when cei.sitename in ('WMYP2') then 'WMY-2'
when cei.sitename in ('WIHK1') then 'WIHK-1'
when cei.sitename in ('WIHK2') then 'WIHK-2'
when cei.sitename in ('WVN') then 'WVN-1'
when cei.sitename in ('WCD') then 'WCD-1'
else cei.sitename end as "plant",
Date(period_start),
indicatoryear,
indicatorm,
indicatorvalue,
NOW() as "last_update_time"
from raw.csr_electricity_indicator cei
where cei.sitename not in ('WKS','WZS')
)




insert into app.electricity_backstage_update
select * from final_table
where date >=date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and date <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and indicatorvalue !=0
and plant not in ('WZS-1','WZS-3','WZS-6','WZS-8','WKS-1','WKS-5','WKS-6');