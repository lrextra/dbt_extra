{{
  config(
    materialized = 'view',
    schema="read_replica"
    )
}}

with source as (

    select
        json_extract_scalar(_airbyte_data, '$.code') as code,
        json_extract_scalar(_airbyte_data, '$.ordre') as ordre,
        json_extract_scalar(_airbyte_data, '$.favori') as favori,
        json_extract_scalar(_airbyte_data, '$.segment') as segment,
        json_extract_scalar(_airbyte_data, '$.photo') as photo,
        json_extract_scalar(_airbyte_data, '$.id_categorie') as id_categorie,
        json_extract_scalar(_airbyte_data, '$.nom') as nom,
        json_extract_scalar(_airbyte_data, '$.categorie_famille_id') as categorie_famille_id,
        _airbyte_emitted_at

    from {{ source('read_replica', '_airbyte_raw_categories') }}
),

final as (

    select
        -- ids
        cast(id_categorie as integer) as id_categorie,
        categorie_famille_id,
    
        -- strings
        nom,
        code,
        favori,
        photo,

        -- timestamps
        _airbyte_emitted_at,

    from source
    order by id_categorie asc
)

select * from final
  
  