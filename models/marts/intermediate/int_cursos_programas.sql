WITH int_cursos AS (
    SELECT * FROM {{ ref('int_cursos') }}
),
int_notas_con_programas AS (
    SELECT * FROM {{ ref('int_notas') }}
),
joined AS (
    SELECT 
        curso_real,
        codigo_curso,
        profesor_curso,
        anio_electivo,
        maestria,
        ROW_NUMBER() OVER (PARTITION BY curso_real ORDER BY curso_real) AS rn
    FROM 
        int_notas_con_programas p
    right JOIN 
        int_cursos c ON p.fk_curso = curso_real
)
SELECT 
    curso_real,
    codigo_curso,
    profesor_curso,
    anio_electivo,
    maestria
FROM 
    joined
WHERE 
    rn = 1
