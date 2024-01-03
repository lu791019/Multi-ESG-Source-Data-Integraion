delete from raw.waste
where period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant not in ('WZS-8','WZS-6','WZS-3','WZS-1','WKS-5','WKS-6A','WKS-6','WKS-1','WKS-6B');

with temp_table as
(
select
case
when cwi.sitename ='WIHK1' then 'WIHK-1'
when cwi.sitename ='WIHK2' then 'WIHK-2'
when cwi.sitename ='WMYP1' then 'WMY-1'
when cwi.sitename ='WMYP2' then 'WMY-2'
else cwi.sitename
end as "plant",
cwi.indicatorid,
case
when cwi.indicatorid = 67 then '一般廢棄物_焚化'
when cwi.indicatorid = 68 then '一般廢棄物_掩埋'
when cwi.indicatorid = 189 then '一般廢棄物_物理處理'
when cwi.indicatorid = 24 then '有害廢棄物'
when cwi.indicatorid = 69 then '一般廢棄物(其他/廚餘)'
when cwi.indicatorid = 85 then '資源廢棄物_推肥'
when cwi.indicatorid = 91 then '資源廢棄物_資源回收'
when cwi.indicatorid = 23 then '資源廢棄物總量'
end as "category",
indicatorvalue as "amount",
date(period_start)  as "period_start"
from raw.csr_waste_indicator cwi
where cwi.indicatorid not in (22,25,50)
)
,


temp_table2 as (
select
plant
,'一般廢棄物(焚化&掩埋)' as "category"
,sum(amount) as "amount"
,period_start
from temp_table tt
where tt.indicatorid in (67,68,189)
group by plant,period_start
union
select
plant
,'有害廢棄物' as "category"
,sum(amount) as "amount"
,period_start
from temp_table tt
where tt.indicatorid in (24)
group by plant,period_start
union
select
plant
,'一般廢棄物(廚餘)' as "category"
,sum(amount) as "amount"
,period_start
from temp_table tt
where tt.indicatorid in (69)
group by plant,period_start
union
select
plant
,'資源廢棄物' as "category"
,sum(amount) as "amount"
,period_start
from temp_table tt
where tt.indicatorid in (23)
group by plant,period_start


)


insert into raw.waste
select
plant
,category
,amount
,'噸' as "unit"
,period_start
,NOW() as "last_update_time"
,'CSR' as "type"
from temp_table2
where
period_start >=  date(to_char(make_date(DATE_PART('YEAR',current_date)::INTEGER-1,01,01),'YYYY-MM-DD'))
and period_start < date(to_char(date_trunc('month', now()) - INTERVAL '1 day','YYYY-MM-DD'))
and plant not in ('WKS','WZS')
;