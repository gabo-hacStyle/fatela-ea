with int_cursos as (
    select 
        curso_real
        , codigo_curso
        , profesor_curso
        , cast(anio_electivo as int) as anio_electivo
    from {{ ref('int_cursos') }}
)
-- asignaturas as (
--     select *
--     from {{ ref('stg_asignaturas') }}
-- ),
-- programas as (
--     select * 
--     from {{ ref('int_programas') }}
-- ),
-- joined as (
--     select *
--     from asignaturas a 
--     left join int_cursos c  on c.codigo_curso = a.codigo_asignatura
--     left join programas p on a.programa = p.nombre_programa
-- ),
-- pk_and_sk as (
--     select 
--         md5(concat_ws('-', programa, codigo_asignatura, anio_electivo)) as sk_registro
--         , concat(codigo_asignatura, '-', anio_electivo) as curso_real
--         , programa
--         ,codigo_asignatura
--         ,nombre_asignatura
--         ,nivel_asignatura
--         ,creditos_asignatura
--         ,profesor_curso
--         ,anio_electivo
--         ,nombre_programa
--         ,codigo_programa
--         ,estado_programa
--     from joined
-- )

select * from int_cursos
