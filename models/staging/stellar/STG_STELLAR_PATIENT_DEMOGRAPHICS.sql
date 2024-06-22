with
    source as (
        select
            patient_id as patient_code,
            age,
            weight,
            last_visit_date,
            created_date,
            updated_date
        from {{ source("stellar", "patient_demographics") }}
    ),
    hashed as (
        select
            {{ dbt_utils.generate_surrogate_key(["patient_code"]) }} as patient_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "patient_code",
                        "age",
                        "weight",
                        "last_visit_date",
                    ]
                )
            }} as patient_hdiff,
            patient_code,
            age,
            weight,
            last_visit_date,
            created_date,
            updated_date as load_ts_utc
        from source
    )
select *
from hashed
