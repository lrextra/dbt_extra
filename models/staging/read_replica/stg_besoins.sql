with source as (

    select
        json_extract_scalar(_airbyte_data, '$.backup') as backup,
        json_extract_scalar(_airbyte_data, '$.extra_id') as extra_id,
        json_extract_scalar(_airbyte_data, '$.qualification_id') as qualification_id,
        json_extract_scalar(_airbyte_data, '$.mission_id') as mission_id,
        json_extract_scalar(_airbyte_data, '$.created_at') as created_at,
        json_extract_scalar(_airbyte_data, '$.releve_etab_debut') as releve_etab_debut,
        json_extract_scalar(_airbyte_data, '$.marge') as marge,
        json_extract_scalar(_airbyte_data, '$.updated_at') as updated_at,
        json_extract_scalar(_airbyte_data, '$.releve_status') as releve_status,
        json_extract_scalar(_airbyte_data, '$.releve_etab_nb_heures') as releve_etab_nb_heures,
        json_extract_scalar(_airbyte_data, '$.releve_facturation_nb_heures') as releve_facturation_nb_heures,
        json_extract_scalar(_airbyte_data, '$.releve_extra_nb_heures') as releve_extra_nb_heures,
        json_extract_scalar(_airbyte_data, '$.id_besoin') as id_besoin,
        json_extract_scalar(_airbyte_data, '$.job_booked_id') as job_booked_id,
        json_extract_scalar(_airbyte_data, '$.commentaire_interne') as commentaire_interne,
        json_extract_scalar(_airbyte_data, '$.releve_etab_pause') as releve_etab_pause,
        json_extract_scalar(_airbyte_data, '$.releve_extra_debut') as releve_extra_debut,
        json_extract_scalar(_airbyte_data, '$.releve_facturation_debut') as releve_facturation_debut,
        json_extract_scalar(_airbyte_data, '$.releve_extra_pause') as releve_extra_pause,
        json_extract_scalar(_airbyte_data, '$.created_by') as created_by,
        json_extract_scalar(_airbyte_data, '$.nb_heures_previsionnel') as nb_heures_previsionnel,
        json_extract_scalar(_airbyte_data, '$.pause') as pause,
        json_extract_scalar(_airbyte_data, '$.type_facturation') as type_facturation,
        json_extract_scalar(_airbyte_data, '$.updated_by') as updated_by,
        json_extract_scalar(_airbyte_data, '$.releve_extra_absent') as releve_extra_absent,
        json_extract_scalar(_airbyte_data, '$.montant_horaire_ht') as montant_horaire_ht,
        json_extract_scalar(_airbyte_data, '$.debut_previsionnel') as debut_previsionnel,
        json_extract_scalar(_airbyte_data, '$.status') as status,
        _airbyte_emitted_at

    from {{ source('read_replica', '_airbyte_raw_besoin') }}
),

final as (

    select
        -- ids
        cast(id_besoin as integer) as id_besoin,
        qualification_id,
        mission_id,
        job_booked_id,

        -- strings
        releve_status,
        commentaire_interne,
        created_by,
        type_facturation,
        updated_by,
        releve_extra_absent,
        status,
        
        -- numerics
        marge,
        releve_etab_nb_heures,
        releve_facturation_nb_heures,
        releve_extra_nb_heures,
        releve_etab_pause,
        releve_extra_pause,
        nb_heures_previsionnel,
        pause,
        montant_horaire_ht,


        -- datetime
        created_at,
        updated_at,
        releve_etab_debut,
        releve_extra_debut,
        releve_facturation_debut,
        debut_previsionnel,
        _airbyte_emitted_at,

    from source
    order by id_besoin desc
)

select * from final
  
  