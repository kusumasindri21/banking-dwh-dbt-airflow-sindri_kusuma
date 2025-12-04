select * from {{ source('banking', 'transactions') }}
