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
                "patient_access_log_code_stellar",
                "p.patient_code_stellar",
                "cr.medical_group_user_code",
                "cr.date_accessed",
                "cr.deleted",
            ]
        )
    }} as patient_access_log_hdiff,
    p.patient_hkey,
    mgu.medical_group_user_hkey,
    cr.date_accessed,
    cr.created_date,
    cr.load_ts_utc,
    cr.deleted
from current_data cr
join {{ ref("REF_STELLAR_PATIENT") }} p on p.patient_code = cr.patient_code
join
    {{ ref("REF_STELLAR_MEDICAL_GROUP_USER") }} mgu
    on mgu.medical_group_user_code = cr.medical_group_user_code
