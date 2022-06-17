with accounts as (
	select * 
	from {{ ref('dim_account') }}
), users as (
	select * 
	from {{ ref('dim_user') }} 
), opens as (
	select * 
	from {{ ref('fct_email_opens') }} 
), joined as (

	select * 
	from users as u
	left join accounts as a
	on u.account_id = a.account_id
	left join opens as o
	on u.lead_id = o.lead_id

)

select 
	account_name,
	count(*) as email_opens
from joined
group by account_name