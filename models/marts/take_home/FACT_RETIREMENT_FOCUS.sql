{{
    config(
        alias="medical_group_dim",
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
        select distinct patient_id, user_id
        from {{ ref("REF_STELLAR_PATIENT_ACCESS_LOG") }}
    ),
    retired_groups as (
        select mgl.medical_group_id, count(*) as retired_patient_count
        from retired_patients rp
        join unique_patient_user pal on pal.patient_id = rp.patient_id
        join
            {{ ref("REF_STELLAR_MEDICAL_GROUP_USER") }} mgu on mgu.user_id = pal.user_id
        join
            {{ ref("REF_STELLAR_MEDICAL_GROUP") }} mgl
            on mgl.medical_group_id = mgu.medical_group_id
        group by mgl.medical_group_id
    ),
    final as (
        select *, rank() over (order by retired_patient_count desc) as ranking
        from retired_groups
    )
select *
from final
