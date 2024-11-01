with int_estudiantes as (
    select 
        cod_es
        , nombre_es
        , genero_es
        , email_es
        , fecha_modificacion
    from {{ ref('int_estudiantes') }}
)

select *
from int_estudiantes 