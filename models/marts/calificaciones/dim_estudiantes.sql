with int_estudiantes as (
    select 
          CAST(cod_es as VARCHAR(255))
        , CAST(nombre_es as VARCHAR(255))
        , CAST(genero_es as VARCHAR(255))
        , CAST(email_es as VARCHAR(255))
        , fecha_modificacion
    from {{ ref('int_estudiantes') }}
)

select *
from int_estudiantes 