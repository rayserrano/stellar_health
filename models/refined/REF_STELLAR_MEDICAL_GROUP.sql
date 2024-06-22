with
    current_data as (
        {{
            current_from_history(
                history_rel=ref("HIST_STELLAR_MEDICAL_GROUP_LIST"),
                key_column="medical_group_hkey",
            )
        }}
    )
select
    {{ get_business_key_source("medical_group_code", "STELLAR", "|") }}
    as medical_group_code_stellar,
    {{ dbt_utils.generate_surrogate_key(["medical_group_code_stellar"]) }}
    as medical_group_hkey,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "medical_group_code_stellar",
                "medical_group_name",
                "state",
                "date_group_created",
                "number_of_users",
                "number_of_patients",
                "deleted",
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
    load_ts_utc,
    deleted
from current_data
