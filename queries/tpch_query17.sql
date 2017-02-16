--  Filter by some part, filter by l_quantity < t_avg_quantity, sum up price

with q17_avg as (
    select
        l_partkey,
        0.2 * avg(l_quantity) as t_avg_quantity
    from
        v_lineitem
        inner join part on l_partkey = p_partkey
    where
        p_brand = 'Brand#23'
        and p_container = 'MED BOX'
    group by
        l_partkey
)

select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
from
    v_lineitem
    inner join part on l_partkey = p_partkey
    inner join q17_avg on q17_avg.l_partkey = v_lineitem.l_partkey
where 
    p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < t_avg_quantity;
