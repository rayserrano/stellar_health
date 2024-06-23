{{
    config(
        alias="retirement_focus_fact",
        unique_key="medical_group_dim_id",
    )
}}

with
    set_retired as (
        select
            patient_hkey, age, case when age >= 67 then true else false end as retired
        from {{ ref("REF_STELLAR_PATIENT") }}
    ),
    retired_patients as (select patient_hkey from set_retired where retired),
    unique_patient_user as (
        select distinct patient_hkey, medical_group_user_hkey,
        from {{ ref("REF_STELLAR_PATIENT_ACCESS_LOG") }}
    ),
    retired_groups as (
        select mg.medical_group_hkey, count(*) as retired_patient_count
        from retired_patients rp
        join unique_patient_user pal on pal.patient_hkey = rp.patient_hkey
        join
            {{ ref("REF_STELLAR_MEDICAL_GROUP_USER") }} mgu
            on mgu.medical_group_user_hkey = pal.medical_group_user_hkey
        join
            {{ ref("REF_STELLAR_MEDICAL_GROUP") }} mg
            on mg.medical_group_hkey = mgu.medical_group_hkey
        group by mg.medical_group_hkey
    ),
    final as (
        select
            medical_group_hkey as medical_group_dim_id,
            retired_patient_count,
            rank() over (order by retired_patient_count desc) as ranking
        from retired_groups
    )
select *
from final
