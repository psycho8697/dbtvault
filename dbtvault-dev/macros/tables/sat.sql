{%- macro sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source, source_model, out_of_sequence=none) -%}

    {{- adapter.dispatch('sat', 'dbtvault')(src_pk=src_pk, src_hashdiff=src_hashdiff,
                                            src_payload=src_payload, src_eff=src_eff, src_ldts=src_ldts,
                                            src_source=src_source, source_model=source_model,
                                            out_of_sequence=out_of_sequence) -}}

{%- endmacro %}

{%- macro default__sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source, source_model, out_of_sequence) -%}

{{- dbtvault.check_required_parameters(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                                       src_ldts=src_ldts, src_source=src_source,
                                       source_model=source_model) -}}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source]) -%}
{%- set rank_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts]) -%}
{%- set pk_cols = dbtvault.expand_column_list(columns=[src_pk]) -%}

{%- if model.config.materialized == 'vault_insert_by_rank' %}
    {%- set source_cols_with_rank = source_cols + [config.get('rank_column')] -%}
{%- endif -%}

{%- if out_of_sequence is not none %}
    {%- set xts_model = out_of_sequence["source_xts"] %}
    {%- set sat_name_col = out_of_sequence["sat_name_col"] %}
    {%- set insert_date = out_of_sequence["insert_date"] %}
    -- depends_on: {{ ref(xts_model) }}
    -- depends_on: {{ this }}
{% endif -%}

{{ dbtvault.prepend_generated_by() }}

WITH source_data AS (
    {%- if model.config.materialized == 'vault_insert_by_rank' %}
    SELECT {{ dbtvault.prefix(source_cols_with_rank, 'a', alias_target='source') }}
    {%- elif out_of_sequence is not none %}
    SELECT DISTINCT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    {%- else %}
    SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    {%- endif %}
    FROM {{ ref(source_model) }} AS a
    WHERE {{ dbtvault.prefix([src_pk], 'a') }} IS NOT NULL
    {%- if model.config.materialized == 'vault_insert_by_period' %}
    AND __PERIOD_FILTER__
    {% elif model.config.materialized == 'vault_insert_by_rank' %}
    AND __RANK_FILTER__
    {% endif %}
),

{% if dbtvault.is_any_incremental() %}

latest_records AS (

    SELECT {{ dbtvault.prefix(rank_cols, 'a', alias_target='target') }},
           RANK() OVER (
               PARTITION BY {{ dbtvault.prefix([src_pk], 'a') }}
               ORDER BY {{ dbtvault.prefix([src_ldts], 'a') }} DESC
           ) AS rank
    FROM {{ this }} AS a
    JOIN (
        SELECT DISTINCT {{ dbtvault.prefix([src_pk], 'source_data') }}
        FROM source_data
    ) AS source_records
    ON {{ dbtvault.prefix([src_pk], 'current_records') }} = {{ dbtvault.prefix([src_pk], 'source_records') }}
    QUALIFY rank = 1
),

out_of_sequence_inserts AS (
  SELECT {{ dbtvault.prefix(source_cols, 'c') }} FROM matching_xts_stg_records AS c
  UNION
  SELECT * FROM records_from_sat
),
{%- endif %}

records_to_insert AS (
    SELECT DISTINCT {{ dbtvault.alias_all(source_cols, 'stage') }}
    FROM source_data AS stage
    {%- if dbtvault.is_any_incremental() %}
    LEFT JOIN latest_records
    ON {{ dbtvault.prefix([src_pk], 'latest_records', alias_target='target') }} = {{ dbtvault.prefix([src_pk], 'stage') }}
    WHERE {{ dbtvault.prefix([src_hashdiff], 'latest_records', alias_target='target') }} != {{ dbtvault.prefix([src_hashdiff], 'stage') }}
        OR {{ dbtvault.prefix([src_hashdiff], 'latest_records', alias_target='target') }} IS NULL
        {% if out_of_sequence is not none -%}
        UNION
        SELECT * FROM out_of_sequence_inserts
        {%- endif %}
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}