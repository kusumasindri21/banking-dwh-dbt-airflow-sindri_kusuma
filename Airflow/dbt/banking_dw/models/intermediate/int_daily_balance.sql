select
    account_id,
    date(transaction_date) as date,
    sum(amount) over (partition by account_id order by transaction_date) as running_balance
from {{ ref('stg_transaction') }}
