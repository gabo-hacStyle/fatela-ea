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
        nombre_curso,
        profesor_curso,
        anio_electivo,
        maestria
    FROM 
        int_notas_con_programas p
    RIGHT JOIN 
        int_cursos c ON p.fk_curso = curso_real
)
SELECT 
    *
FROM 
    joined