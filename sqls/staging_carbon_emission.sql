Delete From staging.carbon_emission
where period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and category ='scope2'
;

insert into staging.carbon_emission
select
distinct
e.plant ,
coalesce((e.amount*ccc.amount) /1000,0) as "amount" ,
e.period_start ,
now() as "last_update_time",
'scope2' as "category"
from raw.electricity_total e
left join staging.cfg_carbon_coef ccc
on to_char(e.period_start,'YYYY')::int4 =  ccc."year"
and e.plant like concat('%',ccc.site,'%')
where period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
;