with int_estudiantes_paises as (
    select pais_estudiante from {{ ref('int_estudiantes') }}
),
joined as (
    select
        ROW_NUMBER() over (order by pais_estudiante) as pk_pais
        , pais_estudiante as pais
    from int_estudiantes_paises group by pais_estudiante 
)

select * from joined 