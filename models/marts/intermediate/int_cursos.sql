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
    
),

real_curso as (
    select 
        concat(codigo_curso, '-', anio_electivo) as curso_real
        , codigo_curso
        , profesor_curso
        , anio_electivo
        
    from codigos_resumidos_segundo_filtro
),
ignoring_identical_registros as (
    select 
        curso_real
        , codigo_curso
        , profesor_curso
        , anio_electivo
        , ROW_NUMBER() OVER (PARTITION BY curso_real) AS rn
    from real_curso
)
select 
        curso_real, codigo_curso, profesor_curso, anio_electivo
    from ignoring_identical_registros where rn = 1

