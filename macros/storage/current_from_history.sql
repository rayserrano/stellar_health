{% macro current_from_history2(
    history_rel,
    key_column,
    selection_expr="*",
    load_ts_column="LOAD_TS_UTC",
    history_filter_expr="true",
    qualify_function="row_number"
) -%}

    select {{ selection_expr }}
    from {{ history_rel }}
    where {{ history_filter_expr }}
    qualify
        {{ qualify_function }} () over (
            partition by {{ key_column }} order by {{ load_ts_column }} desc
        )
        = 1

{%- endmacro %}
