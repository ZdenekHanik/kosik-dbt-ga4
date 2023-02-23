{{
    config(
        pre_hook=[
            "DELETE FROM `ga-kosik-02-2022.dbt_zhanik.test_table` WHERE customer_id_sap = 1039635043;"
        ]
    )
}}

SELECT {{ get_table_suffix(5) }} as id