with renamed as (
    select 
        cod_nombre_curso as codigo_y_nombre_curso
        , profesor_curso
        , substring(periodo from length(periodo) -3 for 4) as anio_electivo
    from {{ source('bd', 'cursos') }}
)
select * from renamed