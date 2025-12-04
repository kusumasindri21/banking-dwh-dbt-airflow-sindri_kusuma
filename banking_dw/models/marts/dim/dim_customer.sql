select distinct
    customer_id,
    first_name || ' ' || last_name as customer_name,
    gender,
    phone,
    registration_date,
    income_level
from {{ ref('stg_customer') }}
