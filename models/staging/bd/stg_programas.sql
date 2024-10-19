with renamed as (
    select 
        nombre_prog as nombre_programa
        , codigo_prog as codigo_programa
        , estado_prog as estado_programa
    from {{ source('bd', 'programas') }}
)

select * from renamed
