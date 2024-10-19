with renamed as (
    select
        fk_programa as programa
        , cod_asig as codigo_asignatura
        , nombre_asig as nombre_asignatura
        , nivel_asig as nivel_asignatura
        , creditos_asig as creditos_asignatura
    from {{ source('bd', 'asignaturas') }}
)
select * from renamed