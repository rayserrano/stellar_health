with
    current_data as (
        {{
            current_from_history(
                history_rel=ref("HIST_STELLAR_PATIENT_DEMOGRAPHICS"),
                key_column="patient_hkey",
            )
        }}
    )
select
    {{ get_business_key_source("patient_code", "STELLAR", "|") }}
    as patient_code_stellar,
    {{ dbt_utils.generate_surrogate_key(["patient_code_stellar"]) }} as patient_hkey,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "patient_code_stellar",
                "age",
                "weight",
                "last_visit_date",
                "deleted",
            ]
        )
    }} as patient_hdiff,
    patient_code,
    age,
    weight,
    last_visit_date,
    created_date,
    load_ts_utc,
    deleted
from current_data
