{#
    DuckDB/Postgres's regexp_replace needs an explicit 'g' flag (4th arg) to
    replace every match; Redshift's regexp_replace takes position/occurrence
    as positional integers in that same slot instead, and already replaces
    every occurrence by default with no flag needed -- passing 'g' there
    fails with "invalid input syntax for integer: 'g'" (confirmed by actually
    running this against Redshift, not by reading the docs).
#}
{% macro regexp_replace_global(string_expr, pattern, replacement) -%}
    {%- if target.name == 'local' -%}
        regexp_replace({{ string_expr }}, {{ pattern }}, {{ replacement }}, 'g')
    {%- else -%}
        regexp_replace({{ string_expr }}, {{ pattern }}, {{ replacement }})
    {%- endif -%}
{%- endmacro %}
