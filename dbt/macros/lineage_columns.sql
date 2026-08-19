{#
    Normalizes the two raw-loading paths' different lineage column names to a
    single staging-level shape (_source_file, _loaded_at), so downstream
    layers don't need to know which path landed a given row -- matching the
    "one raw schema contract" which closes a real gap in: local_loader names its
    lineage columns _source_file/_loaded_at, but Airbyte's file-based source
    always adds its own (_ab_source_file_url/_airbyte_extracted_at) --
    confirmed by a real "column does not exist" error the first time
    dbt build --target redshift actually ran against Airbyte-populated raw
    tables, not caught earlier since that had never happened before.
#}
{% macro lineage_columns() -%}
    {%- if target.name == 'local' -%}
        _source_file,
        _loaded_at
    {%- else -%}
        _ab_source_file_url as _source_file,
        _airbyte_extracted_at as _loaded_at
    {%- endif -%}
{%- endmacro %}
