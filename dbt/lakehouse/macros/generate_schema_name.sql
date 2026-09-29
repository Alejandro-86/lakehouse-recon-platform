{#
    dbt's default behaviour prefixes a model's custom schema with the target
    schema, producing `silver_gold` and `silver_silver`. In a medallion layout
    the layer names *are* the schemas, so take the custom name verbatim and let
    the target schema apply only to models that do not declare one.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
