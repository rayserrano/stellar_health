{% macro save_history_with_multiple_versions(
    input_rel,
    key_column,
    diff_column,
    sort_expr,
    history_rel=this,
    load_ts_column="LOAD_TS_UTC",
    input_filter_expr="true",
    history_filter_expr="true",
    high_watermark_column=none,
    high_watermark_test=">="
) -%}

    {{- config(materialized="incremental") }}
    {%- if execute and not flags.FULL_REFRESH %}
        {% set incremental_w_external_input = history_rel != this %}
    {% endif -%}

    with
        {% if is_incremental() or incremental_w_external_input %}

            {%- set selection_expression %}
    {{key_column}}, {{diff_column}}
    , row_number() OVER( PARTITION BY {{key_column}}, {{load_ts_column}} ORDER BY {{sort_expr}}) as rn
    , count(*) OVER( PARTITION BY {{key_column}}, {{load_ts_column}}) as cnt
            {%- endset -%}

            current_from_history as (
                {{
                    current_from_history(
                        history_rel=history_rel,
                        key_column=key_column,
                        selection_expr=selection_expression,
                        load_ts_column=load_ts_column,
                        history_filter_expr=history_filter_expr,
                        qualify_function="(rn = cnt) and rank",
                    )
                }}
            ),

            load_from_input as (
                select
                    i.*,
                    lag(i.{{ diff_column }}) over (
                        partition by i.{{ key_column }} order by {{ sort_expr }}
                    ) as prev_hdiff,
                    case
                        when prev_hdiff is null
                        then coalesce(i.{{ diff_column }} != h.{{ diff_column }}, true)
                        else (i.{{ diff_column }} != prev_hdiff)
                    end as to_be_stored
                from {{ input_rel }} as i
                left outer join
                    current_from_history as h on h.{{ key_column }} = i.{{ key_column }}
                where
                    {{ input_filter_expr }}
                    {%- if high_watermark_column %}
                        and {{ high_watermark_column }} {{ high_watermark_test }} (
                            select max({{ high_watermark_column }})
                            from {{ history_rel }}
                        )
                    {%- endif %}
            )

        {%- else %}
            load_from_input as (
                select
                    i.*,
                    lag(i.{{ diff_column }}) over (
                        partition by i.{{ key_column }} order by {{ sort_expr }}
                    ) as prev_hdiff,
                    case
                        when prev_hdiff is null
                        then true
                        else (i.{{ diff_column }} != prev_hdiff)
                    end as to_be_stored
                from {{ input_rel }} as i
                where {{ input_filter_expr }}
            )
        {%- endif %}

    select * exclude(prev_hdiff, to_be_stored)
    from load_from_input
    where to_be_stored
    order by {{ key_column }}, {{ sort_expr }}

{%- endmacro %}
