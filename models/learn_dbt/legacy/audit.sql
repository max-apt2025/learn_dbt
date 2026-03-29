------------------------------------------------------
-->>>>>>compare row count <<<<<<<<<--
------------------------------------------------------

{# {% set old_relation = adapter.get_relation(
      database = target.database,
      schema = "fact_legacy",
      identifier = "customer_orders_legacy"
) -%}

{% set dbt_relation = ref('fact_customer_orders') %}

{{ _helper.compare_row_counts(
    a_relation = old_relation,
    b_relation = dbt_relation
) }}
------------------------------------------------------
-->>>>>>compare relational columns <<<<<<<<<--
------------------------------------------------------

{% set old_relation = adapter.get_relation(
      database = target.database,
      schema = "fact_legacy",
      identifier = "customer_orders_legacy"
) -%}

{% set dbt_relation = ref('fact_customer_orders') %}

{{ _helper.compare_relation_columns(
    a_relation=old_relation,
    b_relation=dbt_relation
) }}
------------------------------------------------------
-->>>>>>compare all columns <<<<<<<<<--
------------------------------------------------------

{% set old_relation = adapter.get_relation(
      database = target.database,
      schema = "fact_legacy",
      identifier = "customer_orders_legacy"
) -%}

{% set dbt_relation = ref('fact_customer_orders') %}

{{ _helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "order_id"
) }} #}
