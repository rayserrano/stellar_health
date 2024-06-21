{{ config(materialized="ephemeral") }}

with
    source as (
        select
            medical_group_id as medical_group_code,
            groupname as medical_group_name,
            state,
            date_group_created,
            number_of_users,
            number_of_patients,
            created_date,
            updated_date
        from {{ source("stellar", "medical_group_list") }}
    ),
    hashed as (
        select
            {{ dbt_utils.generate_surrogate_key(["medical_group_code"]) }}
            as medical_group_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "medical_group_code",
                        "medical_group_name",
                        "state",
                        "date_group_created",
                        "number_of_users",
                        "number_of_patients",
                    ]
                )
            }} as medical_group_hdiff,
            medical_group_code,
            medical_group_name,
            state,
            date_group_created,
            number_of_users,
            number_of_patients,
            created_date,
            updated_date as load_ts_utc
        from source
    )
select *
from hashed
