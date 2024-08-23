-- macros/generate_hash_key.sql
{% macro generate_hash_key(fields) %}
    md5(concat({{ fields | join(', ') }}))
{% endmacro %}
