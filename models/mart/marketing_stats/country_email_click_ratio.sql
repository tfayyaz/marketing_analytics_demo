with accounts as (
	select * 
	from {{ ref('dim_account') }}
), users as (
	select * 
	from {{ ref('dim_user') }} 
), opens as (
	select * 
	from {{ ref('fct_email_opens') }} 
), clicks as (
	select * 
	from {{ ref('fct_email_clicks') }} 
), opens_clicks_joined as (

    select 
    o.lead_id as lead_id,
    o.activity_timestamp as open_ts,
    c.activity_timestamp as click_ts
    from opens as o 
    left join clicks as c 
    on o.email_send_id = c.email_send_id
    and o.lead_id = c.lead_id

), joined as (

	select * 
	from users as u
	left join accounts as a
	on u.account_id = a.account_id
	left join opens_clicks_joined as oc
	on u.lead_id = oc.lead_id

)

select 
	billing_country as country,
	count(open_ts) as opens,
	count(click_ts) as clicks,
	count(click_ts) / count(open_ts) as click_ratio
from joined
group by country