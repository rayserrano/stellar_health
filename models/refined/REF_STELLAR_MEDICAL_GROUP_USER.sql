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
    {{ get_business_key_source("cr.medical_group_user_code", "STELLAR", "|") }}
    as medical_group_user_code_stellar,
    {{ dbt_utils.generate_surrogate_key(["medical_group_user_code_stellar"]) }}
    as medical_group_user_hkey,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "medical_group_user_code_stellar",
                "cr.medical_group_user_name",
                "cr.medical_group_code",
                "cr.password",
                "cr.user_role",
                "cr.date_user_created",
                "cr.last_login_date",
                "cr.deleted",
            ]
        )
    }} as medical_group_user_hdiff,
    cr.medical_group_user_code,
    cr.medical_group_user_name,
    mg.medical_group_hkey,
    cr.password,
    cr.user_role,
    cr.date_user_created,
    cr.last_login_date,
    cr.created_date,
    cr.load_ts_utc,
    cr.deleted
from current_data cr
join
    {{ ref("REF_STELLAR_MEDICAL_GROUP") }} mg
    on mg.medical_group_code = cr.medical_group_code
