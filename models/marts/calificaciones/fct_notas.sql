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
        ,n.fk_estudiante
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
    right join dim_cursos c on n.fk_curso = c.curso_y_maestria 
    left join dim_estudiantes e on n.fk_estudiante = e.cod_es
    left join int_estudaintes_pais ep on ep.fk_estudiante = e.cod_es
    left join dim_paises p on p.pk_pais = ep.fk_pais
),
 
llave_sk as (
    select 
        md5(concat_ws('-', 
        fk_curso,
         fk_estudiante)) as sk_nota
        , *
    from joined
),
filtrar_no_nulos as (
    select * from llave_sk where fk_estudiante is not null
)
select 
    cast(sk_nota as varchar(255))
    , cast(fk_curso as varchar(255)) 
    ,cast(fk_estudiante as varchar(255)) 
    ,cast(fk_pais as int) 
    ,cast(codigo_curso as varchar(255)) 
    ,cast(nombre_curso as varchar(255)) 
    ,cast(aprobado as varchar(255)) 
    ,cast(nota as real) 
    ,cast(status as varchar(255)) 
    ,cast(anio_electivo as int) 
    ,cast(nombre_es as varchar(255)) 
    ,cast(genero_es as varchar(255)) 
    ,cast(email_es as varchar(255)) 
    ,cast(pais as varchar(255)) 
    ,cast(profesor_curso as varchar(255)) 
    ,cast(maestria as varchar(255)) 


 from filtrar_no_nulos 