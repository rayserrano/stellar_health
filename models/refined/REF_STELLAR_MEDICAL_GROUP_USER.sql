with
    current_data as (
        {{
            current_from_history(
                history_rel=ref("HIST_STELLAR_MEDICAL_GROUP_USER"),
                key_column="medical_group_user_hkey",
            )
        }}
    )
select
    {{ get_business_key_source("medical_group_user_code", "STELLAR", "|") }}
    as medical_group_user_code_stellar,
    {{ dbt_utils.generate_surrogate_key(["medical_group_user_code_stellar"]) }}
    as medical_group_user_hkey,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "medical_group_user_code_stellar",
                "medical_group_user_name",
                "medical_group_code",
                "password",
                "user_role",
                "date_user_created",
                "last_login_date",
                "deleted",
            ]
        )
    }} as medical_group_hdiff,
    medical_group_user_name,
    medical_group_code,
    password,
    user_role,
    date_user_created,
    last_login_date,
    created_date,
    load_ts_utc,
    deleted
from current_data
