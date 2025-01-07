with int_notas as (
    select *
    from {{ ref('int_notas') }}
),
dim_cursos as (
    select * 
    from {{ ref('dim_cursos') }}
),
dim_estudiantes as (
    select *
    from {{ ref('dim_estudiantes') }}
),
int_estudaintes_pais as (
    select * 
    from {{ ref('int_estudiante_pais') }}
),
dim_paises as (
    select *
    from {{ ref('dim_paises') }}
),
joined as (
    select 
         fk_curso
        , n.fk_estudiante
        , fk_pais
        , codigo_curso
        , nombre_curso
        , aprobado
        , nota
        , status
        , anio_electivo
        , nombre_es
        , genero_es
        , email_es
        , pais
        , profesor_curso
        , n.maestria

        

    from int_notas n
    right join dim_cursos c on c.curso_real = n.fk_curso
    left join dim_estudiantes e on n.fk_estudiante = e.cod_es
    left join int_estudaintes_pais ep on ep.fk_estudiante = e.cod_es
    left join dim_paises p on p.pk_pais = ep.fk_pais
),
 
llave_sk as (
    select 
        md5(concat_ws('-', fk_curso, fk_estudiante, maestria))
        , *
    from joined
),
filtrar_no_nulos as (
    select * from llave_sk where fk_estudiante is not null
)
select * from filtrar_no_nulos 