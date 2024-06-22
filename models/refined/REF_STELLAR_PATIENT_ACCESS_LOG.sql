with
    current_data as (
        {{
            current_from_history(
                history_rel=ref("HIST_STELLAR_PATIENT_ACCESS_LOG"),
                key_column="patient_access_log_hkey",
            )
        }}
    )
select
    {{ get_business_key_source("patient_access_log_code", "STELLAR", "|") }}
    as patient_access_log_code_stellar,
    {{ dbt_utils.generate_surrogate_key(["patient_access_log_code_stellar"]) }}
    as patient_access_log_hkey,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "patient_access_log_code",
                "patient_code",
                "medical_group_user_code",
                "date_accessed",
                "deleted",
            ]
        )
    }} as patient_hdiff,
    patient_access_log_code,
    patient_code,
    medical_group_user_code,
    date_accessed,
    created_date,
    load_ts_utc,
    deleted
from current_data
