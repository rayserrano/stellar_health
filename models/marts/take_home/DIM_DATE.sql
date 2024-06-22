{{
    config(
        alias="date_dim",
        materialized="incremental",
        unique_key="date_dim_id",
    )
}}

with core_date as ({{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }})
select {{ dbt_utils.generate_surrogate_key(["date_day"]) }} as date_dim_id, *
from core_date
