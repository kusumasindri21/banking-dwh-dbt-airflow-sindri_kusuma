select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_date,
    t.amount,
    t.transaction_type
from {{ ref('stg_transaction') }} as t
left join {{ ref('stg_account') }} as a
    on t.account_id = a.account_id
