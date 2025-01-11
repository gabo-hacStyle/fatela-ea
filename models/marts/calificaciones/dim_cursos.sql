with int_cursos_programas as (
    select
        cast(curso_y_maestria as varchar(255)) 
        ,cast(curso_real as VARCHAR(255)) 
        , cast(codigo_curso as VARCHAR(255)) 
        , cast(nombre_curso as VARCHAR(255)) 
        , cast(profesor_curso as VARCHAR(255)) 
        , cast(maestria as VARCHAR(255)) 
        , cast(anio_electivo as int) as anio_electivo
        
    from {{ ref('int_cursos_programas') }}
)


select * from int_cursos_programas 
