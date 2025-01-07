with  renamed as (
    select 
        case when fk_estudiante is null then 'Estudiante no registrado' else fk_estudiante end as fk_estudiante    
        , substring(periodo from length(periodo) - 3 for 4) as anio_electivo
        , asignatura
        , aprobado
        , nota
        , status
        , maestria
    from {{ source('bd', 'notas') }}
)

select * from renamed 



