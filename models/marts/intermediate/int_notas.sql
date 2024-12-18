with int_notas as (
    select 
        fk_estudiante
        , anio_electivo
        , SUBSTRING(
            asignatura 
            FROM 2 
            FOR POSITION(')' IN asignatura) - 2
        ) AS asignatura
        , aprobado
        , nota
        , status
        , maestria
    from {{ ref('stg_notas') }}
    
),
fixing_one as (
    select 
    fk_estudiante
        , anio_electivo
    , case 
        when asignatura = 'MS 628 ' then 'MS 628'
        when asignatura = 'TE-601' or asignatura = 'TE- 601' then 'TE 601'
        else asignatura
    end as asignatura
    , aprobado
        , nota
        , status
        , maestria
    from int_notas
), 
fk_curso as (
    select 
        concat(asignatura, '-', anio_electivo) as fk_curso
        , fk_estudiante
        , aprobado
        , nota
        , status
        , maestria
    from fixing_one
)

select  * from fk_curso 
