
-- En modelos de staging casteamos los datos a su tipo correcto
-- y renombramos las columnas

-- para 'estudiantes', definimos un estandar para fecha_modificacion asi:

with
    estudiantes_sin_fecha as (
        select cod_es
            , nombre_es
            , genero_es
            , email_es
            , pais_es
            , cast(fecha_modificacion as date) 
        from {{ source("bd", "estudiantes") }}
        where fecha_modificacion is null 
    ),
   
    fecha_normal as (
        select cod_es
            , nombre_es
            , genero_es
            , email_es
            , pais_es
            , fecha_modificacion as fecha_modificada

        from {{ source("bd", "estudiantes") }}
        where length(fecha_modificacion) > 9

    ),
    fecha_menos_de_nueve as (
        select *

        from {{ source("bd", "estudiantes") }}
        where length(fecha_modificacion) < 9

    ),
    fecha_menos_de_diez as (
        select *

        from {{ source("bd", "estudiantes") }}
        where length(fecha_modificacion) = 9
    ),
    fecha_u9_con_zeros as (
        select 
            cod_es
            , nombre_es
            , genero_es
            , email_es
            , pais_es 
            , concat(
                0,
                substring(fecha_modificacion, 1, 2),
                0,
                substring(fecha_modificacion, 3)
            ) as fecha_modificada
        from fecha_menos_de_nueve
    ),
    fecha_u10_con_zeros as (
        select
            cod_es
            , nombre_es
            , genero_es
            , email_es
            , pais_es
            , case 
                when POSITION('/' IN fecha_modificacion) = 2 then concat(
                    0,
                    substring(fecha_modificacion, 1, 2),
                    substring(fecha_modificacion, 3)
                ) else concat(
                substring(fecha_modificacion, 1, 3),
                0,
                substring(fecha_modificacion,4)
            ) end as fecha_modificada
        from fecha_menos_de_diez
    ), 
    union_fechas as (
        select 
            *
        from fecha_u9_con_zeros 
        union 
        select * from fecha_u10_con_zeros
        union
        select * from fecha_normal
        
    ),
    casted as ( 
        
        select * from estudiantes_sin_fecha
        union
        select 
            cod_es
            , nombre_es
            , genero_es
            , email_es
            , pais_es
            , cast(concat(
                substring(fecha_modificada, 7, 4),
                substring(fecha_modificada, 3, 4),
                substring(fecha_modificada, 1, 2)
            ) as date ) as fecha_modificacion
        
        from union_fechas
    
    )


select * from casted