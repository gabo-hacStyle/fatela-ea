WITH int_cursos AS (
    SELECT * FROM {{ ref('int_cursos') }}
),
int_notas_con_programas AS (
    SELECT * FROM {{ ref('int_notas') }}
),

rn_assigned as (
    select 
        curso_anio
        , maestria
        , fk_curso
        , ROW_NUMBER() OVER (PARTITION BY fk_curso ) AS rn
    from int_notas_con_programas
),
filtering_one_result as (
    select 
        curso_anio
        , maestria
        , fk_curso 
    from rn_assigned where rn = 1
),

joined AS (
    SELECT 
        fk_curso as curso_y_maestria,
        curso_real,
        codigo_curso,
        nombre_curso,
        profesor_curso,
        anio_electivo,
        maestria
    FROM 
        int_cursos c
    left JOIN 
        filtering_one_result np ON np.curso_anio = c.curso_real
)
SELECT 
    *
FROM 
    joined
    