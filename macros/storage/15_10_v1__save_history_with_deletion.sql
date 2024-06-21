{% macro save_history_with_deletion(
    input_rel, key_column, diff_column, load_ts_column="LOAD_TS_UTC"
) -%}

    {{ config(materialized="incremental") }}

    with

        {% if is_incremental() %}  -- in an incremental run and the dest table already exists
            current_from_history as (
                {{-
                    current_from_history(
                        history_rel=this,
                        key_column=key_column,
                        load_ts_column=load_ts_column,
                    )
                }}
            ),

            load_from_input as (
                select
                    i.* exclude ({{ load_ts_column }}),
                    i.{{ load_ts_column }},
                    false as deleted
                from {{ input_rel }} as i
                left outer join
                    current_from_history curr
                    on (
                        not curr.deleted
                        and i.{{ diff_column }} = curr.{{ diff_column }}
                    )
                where curr.{{ diff_column }} is null
            ),
            deleted_from_hist as (
                select
                    curr.* exclude (deleted, {{ load_ts_column }}),
                    '{{ run_started_at }}' as {{ load_ts_column }},
                    true as deleted
                from current_from_history curr
                left outer join
                    {{ input_rel }} as i on (i.{{ key_column }} = curr.{{ key_column }})
                where not curr.deleted and i.{{ key_column }} is null
            ),

            changes_to_store as (
                select *
                from load_from_input
                union all
                select *
                from deleted_from_hist
            )
        {%- else %}  -- not an incremental run (like a full-refresh) or the dest table does not yet exists
            changes_to_store as (
                select
                    i.* exclude ({{ load_ts_column }}),
                    {{ load_ts_column }},
                    false as deleted
                from {{ input_rel }} as i
            )
        {%- endif %}

    select *
    from changes_to_store

{% endmacro %}
