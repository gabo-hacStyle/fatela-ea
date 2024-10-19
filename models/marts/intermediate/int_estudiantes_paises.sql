with estudiantes_con_pais as (
    select 
            cod_es
            , nombre_es
            , genero_es
            , email_es        
        , case 
            when UPPER(pais_es) LIKE UPPER('%per%')
                or UPPER(pais_es) LIKE UPPER('%jr.%')  
                OR pais_es = 'Los Olivos' 
                OR pais_es = 'Benjamin Castañeda 218' 
                -- or pais_es = 'Jr. Lima 746' 
                -- or pais_es ='Jr. Amazonas 807' 
                or pais_es = 'Garcilazo de la Vega 795'
                or pais_es = 'Trujillo'
                or pais_es = 'Trujjillo'
                or pais_es = 'Arequipa'
                or pais_es = 'Miraflores'
                or pais_es = 'Av. Prolongacion Grau 1945'
                or pais_es = 'Pucallpa'
                or pais_es = 'Francisco Cabrera 050-206'
                or pais_es = 'Comas'
                or pais_es = 'Av. Los Algarrobos 429'
                or pais_es = 'Maximo Alvarado 384'
                -- or pais_es = 'Jr. Saturno 125'
                -- or pais_es = 'Jr. San Ignacio 260'
                or pais_es = 'Urb. El Pacifico Calle Payet'
                or pais_es = 'Chimbote'
                or pais_es = 'Huanuco'
                or pais_es = 'Lima'
                or pais_es = 'Barlovento 473'
                or cod_es = '10944'
                or cod_es = '10772'
                or cod_es = '10773'
                or cod_es = '10927'
                or cod_es = '10946'
                or cod_es = '11074'
                or cod_es = '11220'
                or cod_es = '11238'
                or cod_es = '11250'
            then 'Perú'
            when UPPER(pais_es) LIKE UPPER('%REP%')
                or UPPER(pais_es) LIKE UPPER('%manolo%')  
                OR pais_es = 'R.D.' 
                or pais_es = 'Rd' 
                or pais_es = 'RD'
                or pais_es = 'R.d.'
                
            then 'Republica Dominicana'

            when UPPER(pais_es) LIKE UPPER('%CHILE%') 
                or pais_es = 'Santiago' 
                or pais_es = 'Temuco'
                or pais_es = 'nest537@gmail.com'
                or cod_es = '10769'
                or cod_es = '10929'
                or cod_es = '10997'
                or cod_es = '11081'
                or cod_es = '11196'
                or cod_es = '11207'
                or cod_es = '11219'
                or cod_es = '11230'
                or cod_es = '11233'
                or cod_es = '10664'
                or cod_es = '10665'
                or cod_es = '10667'
            then 'Chile'
            when pais_es = 'españa' 
                or cod_es = '11082'
                or cod_es = '11092'
            then 'España' 
            when pais_es = 'Ambato' 
                or UPPER(pais_es) LIKE UPPER('%quil%') 
                or UPPER(pais_es) LIKE UPPER('%guaya%') 
                or pais_es = 'Portoviejo' 
                or pais_es = 'Quito'
                or UPPER(pais_es) LIKE UPPER('%Claveles%')
                or pais_es = 'Ecaudor'
                or pais_es = 'Mexico 541'
                or pais_es = 'Quevedo'
                or cod_es = '11197'
                or cod_es = '11200'
            then 'Ecuador'
            when pais_es = 'Sao Paulo' 
                or cod_es = '10626'
            then 'Brasil'
            when pais_es = 'USA' 
                or cod_es = '10993'
                or cod_es = '11156'
                or cod_es = '10910'
            then 'Estados Unidos'
            when pais_es = 'Buenos Aires'
                or cod_es = '11194'
            then 'Argentina'
            when cod_es = '11195' then 'Australia'
            when cod_es = '11000'
                or cod_es = '11001'
            then 'Bolivia' 
            when cod_es = '10643'
                or cod_es = '10668'
                or cod_es = '11235'
            then 'Colombia'
            when cod_es = '11158' then 'Guatemala'
            when cod_es = '10909' 
                or cod_es = '10663'
                or pais_es is null
            then 'No registra' 
            else
             pais_es end as
        pais_estudiante
        , fecha_modificacion
        
    from {{ ref('stg_estudiantes') }}
    

)

select * from estudiantes_con_pais 
