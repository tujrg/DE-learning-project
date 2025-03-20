{% macro create_external_table_from_delta(table_name, gcs_path, project_id, dataset_id) %}

  {% set sql %}
    CREATE OR REPLACE EXTERNAL TABLE `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
    OPTIONS (
      format = 'DELTA_LAKE',
      uris = ['{{ gcs_path }}']
    );
  {% endset %}

  {{ log("Creating external table: " ~ project_id ~ "." ~ dataset_id ~ "." ~ table_name, info=True) }}
  {{ run_query(sql) }}

{% endmacro %}
