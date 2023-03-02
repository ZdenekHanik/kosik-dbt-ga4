{%- macro get_table_suffix(subtract_days) -%}
    FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Europe/Prague"), INTERVAL {{ subtract_days }} DAY))
{%- endmacro -%}

{%- macro get_delete_statement(table, days) -%}
    DELETE FROM {{ table }} WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE('Europe/Prague'), INTERVAL {{ days }} DAY) AND DATE_SUB(CURRENT_DATE('Europe/Prague'), INTERVAL 1 DAY);
{%- endmacro -%}