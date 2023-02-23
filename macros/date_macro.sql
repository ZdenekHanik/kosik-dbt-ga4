{%- macro get_table_suffix(subtract_days) -%}
    FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Europe/Prague"), INTERVAL {{ subtract_days }} DAY))
{%- endmacro -%}