with
    source as (
        select
            user_id as medical_group_user_code,
            username as medical_group_user_name,
            medical_group_id as medical_group_code,
            password,
            "ROLE" as user_role,
            date_user_created,
            last_login_date,
            created_date,
            updated_date
        from {{ source("stellar", "medical_group_user") }}
    ),
    hashed as (
        select
            {{ dbt_utils.generate_surrogate_key(["medical_group_user_code"]) }}
            as medical_group_user_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "medical_group_user_code",
                        "medical_group_user_name",
                        "medical_group_code",
                        "password",
                        "user_role",
                        "date_user_created",
                        "last_login_date",
                    ]
                )
            }} as medical_group_user_hdiff,
            medical_group_user_code,
            medical_group_user_name,
            medical_group_code,
            password,
            user_role,
            date_user_created,
            last_login_date,
            created_date,
            updated_date as load_ts_utc
        from source
    )
select *
from hashed
