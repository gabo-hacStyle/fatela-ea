with int_programas as  ( 
    select *
    from {{ ref('int_programas') }}
)

select * from int_programas