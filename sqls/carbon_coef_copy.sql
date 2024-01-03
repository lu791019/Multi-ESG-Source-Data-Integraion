with tmp as (
select
cast (to_char((SELECT now()::timestamp+'1 year'),'yyyy') as INTEGER) as "year",
site,
amount,
now() as "last_update_time"
from staging.cfg_carbon_coef ccc 
where "year" = cast (to_char((SELECT now()::timestamp),'yyyy') as INTEGER)

)

insert into staging.cfg_carbon_coef(year,site,amount,last_update_time)
select
* from tmp;