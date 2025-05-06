with generated_date as (
{{ dbt_utils.date_spine( 
        datepart="day",
        start_date="'2015-01-01'::date",
        end_date="'2030-01-01'::date"
    )
    }}
)

select
    date_day::date
from generated_date