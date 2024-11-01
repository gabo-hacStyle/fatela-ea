with paises as (
    select * from {{ ref('dim_paises') }}
),
estudiantes as (
    select * from {{ ref('int_estudiantes') }}
),
joined as (
    select 
        cod_es as fk_estudiante
        , pk_pais as fk_pais
    from paises p
    left join estudiantes e on p.pais = e.pais_estudiante
)

select * from joined
