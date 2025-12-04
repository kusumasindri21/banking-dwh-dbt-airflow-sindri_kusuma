select * from {{ source('banking', 'customers') }}
