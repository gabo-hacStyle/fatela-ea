with codigos_resumidos as (
    select 
    SUBSTRING(
        codigo_y_nombre_curso 
            FROM 1 FOR LENGTH(codigo_y_nombre_curso) - POSITION('-' IN REVERSE(codigo_y_nombre_curso)) - 1) 
        AS  codigo_curso 
    , profesor_curso
    , anio_electivo
    from {{ ref('stg_cursos') }}
), 
codigos_resumidos_segundo_filtro as (
    select 
        case 
            when codigo_curso = 'TP 613EC' then 'TP 613'
            when codigo_curso = 'TP 614 - (T' then 'TP 614' 
            when codigo_curso = 'TP 642 - Des. del Pensam. Teológico s.XVI' then 'TP 642'
            WHEN POSITION('-' IN codigo_curso) > 0 THEN SUBSTRING(codigo_curso FROM POSITION('-' IN codigo_curso) + 1)
            else codigo_curso 
        end as codigo_curso 
        , profesor_curso
        , anio_electivo
    from codigos_resumidos
)
select * from codigos_resumidos_segundo_filtro 
