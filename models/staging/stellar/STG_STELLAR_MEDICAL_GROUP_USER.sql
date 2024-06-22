{{ config(materialized="ephemeral") }}

select *
from {{ source("stellar", "medical_group_user") }}
