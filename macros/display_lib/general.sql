{%- macro get_business_key_source(column_value, source_code, delimiter_value) -%}

    {% do return(
        "concat('"
        ~ source_code
        ~ "','"
        ~ delimiter_value
        ~ "' ,"
        ~ column_value
        ~ ")"
    ) %}
{%- endmacro %}
