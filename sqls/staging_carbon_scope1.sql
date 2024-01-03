Delete
From staging.carbon_emission
where period_start >=date(to_char(date_trunc('year', now()) - INTERVAL '1 year','YYYY-MM-DD'))
and period_start <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and category ='scope1'
and plant like ('%WZS%') ;

Delete
From staging.carbon_emission
where period_start >=date(to_char(date_trunc('year', now()) - INTERVAL '1 year','YYYY-MM-DD'))
and period_start <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and category ='scope1'
and plant like ('%WKS%') ;


with temp_table as
(
select
'WZS' as "site",
plant, amount, period_start from staging.carbon_emission ce
where period_start >=date(to_char(date_trunc('year', now()) - INTERVAL '1 year','YYYY-MM-DD'))
and period_start <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant like ('%WZS%')
and ce.category ='scope2'
union
select
'WKS' as "site",
plant, amount, period_start from staging.carbon_emission ce
where period_start >=date(to_char(date_trunc('year', now()) - INTERVAL '1 year','YYYY-MM-DD'))
and period_start <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant like ('%WKS%')
and ce.category ='scope2'
)
,



summ as (
select
site,
period_start,
sum(amount) as "summary"
from temp_table
group by period_start,site
)
,




ratio as (
select
tt.site,
tt.plant,
tt.period_start,
tt.amount/st.summary as "ratio"
from temp_table tt
left join summ st
on tt.site = st.site
and tt.period_start = st.period_start
)
,



temp_scope1 as (
select plant,amount,period_start
from raw.carbon_emission ce
where period_start >=date(to_char(date_trunc('year', now()) - INTERVAL '1 year','YYYY-MM-DD'))
and period_start <= date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant in ('WZS','WKS')
and ce.category ='scope1'
)
,



final_table as (
select
r.plant,
ts.amount * r.ratio as "amount",
ts.period_start,
now() as "last_update_time",
'scope1' as "category"
from temp_scope1 ts
left join ratio r
on ts.plant = r.site
and ts.period_start = r.period_start
)


insert into staging.carbon_emission
select
*
from final_table
;