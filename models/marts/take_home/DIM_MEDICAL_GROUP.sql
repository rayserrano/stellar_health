{{
    config(
        alias="medical_group_dim",
        materialized="incremental",
        unique_key="medical_group_dim_id",
    )
}}

select
    medical_group_hkey as medical_group_dim_id,
    medical_group_name,
    state,
    date_group_created,
    number_of_users,
    number_of_patients,
    created_date,
    '{{ run_started_at }}'::timestamp as load_ts_utc,
    deleted
from {{ ref("REF_STELLAR_MEDICAL_GROUP") }}
