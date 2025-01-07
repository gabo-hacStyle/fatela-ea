with int_cursos_programas as (
    select 
        curso_real
        , codigo_curso
        , profesor_curso
        , cast(anio_electivo as int) as anio_electivo
        , maestria
    from {{ ref('int_cursos_programas') }}
)


select * from int_cursos_programas 
