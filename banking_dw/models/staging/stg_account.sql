select * from {{ source('banking', 'accounts') }}
