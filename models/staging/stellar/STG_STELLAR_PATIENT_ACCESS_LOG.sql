with
    source as (
        select
            patient_access_log_id as patient_access_log_code,
            patient_id as patient_code,
            user_id as medical_group_user_code,
            date_accessed,
            created_date,
            updated_date
        from {{ source("stellar", "patient_access_log") }}
    ),
    hashed as (
        select
            {{ dbt_utils.generate_surrogate_key(["patient_access_log_code"]) }}
            as patient_access_log_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "patient_access_log_code",
                        "patient_code",
                        "medical_group_user_code",
                        "date_accessed",
                    ]
                )
            }} as patient_access_log_hdiff,
            patient_access_log_code,
            patient_code,
            medical_group_user_code,
            date_accessed,
            created_date,
            updated_date as load_ts_utc
        from source
    )
select *
from hashed
