/**
 *  Important usage notes:
 *  1. When a Key is marked deleted in the HIST table, it will be added again as not deleted, if it's presented in the STG input.
 *  2. When a Key is not marked deleted in the HIST table, it will be added again marked deleted, if it's presented in the DEL input.
 *  3. The combined result of #1 and #2 is that if a key is present in both STG and DEL input, the result will depend on its state in HIST.
 *     If the key is delete, it will be undeleted, while if it was not delete, it will be deleted.
 *  4. A priority must be estabilished between the two inputs, if we want to remove this alternance.
 *     Giving priority to the STG input, it would mean that keys appearing in STG would be removed from the deletion list.
 *     Giving priority to the DEL input, it would mean that keys appearing in DEL list would be removed from the STG input.
 *     The above rules can be implemented in the STG model being fed to the macro.
 */
{% macro save_history_with_deletion_from_list(
    input_rel,
    key_column,
    diff_column,
    del_rel,
    del_key_column,
    load_ts_column="LOAD_TS_UTC",
    input_filter_expr="true",
    high_watermark_column=none,
    high_watermark_test=">=",
    order_by_expr=none
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
                where
                    curr.{{ diff_column }} is null and {{ input_filter_expr }}
                    {%- if high_watermark_column %}
                        and {{ high_watermark_column }} {{ high_watermark_test }} (
                            select max({{ high_watermark_column }}) from {{ this }}
                        )
                    {%- endif %}
            ),
            delete_from_hist as (
                select
                    curr.* exclude (deleted, {{ load_ts_column }}),
                    '{{ run_started_at }}' as {{ load_ts_column }},
                    true as deleted
                from current_from_history curr
                where
                    not curr.deleted
                    and curr.{{ key_column }}
                    in (select {{ del_key_column }} from {{ del_rel }})
            ),

            changes_to_store as (
                select *
                from load_from_input
                union all
                select *
                from delete_from_hist
            )
        {%- else %}  -- not an incremental run (like a full-refresh) or the dest table does not yet exists
            changes_to_store as (
                select
                    i.* exclude ({{ load_ts_column }}),
                    {{ load_ts_column }},
                    false as deleted
                from {{ input_rel }} as i
                where {{ input_filter_expr }}
            )
        {%- endif %}

    select *
    from changes_to_store
    {%- if order_by_expr %} order by {{ order_by_expr }} {%- endif %}

{% endmacro %}
