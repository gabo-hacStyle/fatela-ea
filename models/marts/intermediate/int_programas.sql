with int_programas as (
    select distinct *
    from {{ ref('stg_programas') }}
)

select * from int_programas where length(nombre_programa) > 11