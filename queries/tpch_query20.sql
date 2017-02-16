--  For part like 'forest%' and supplier in CANADA, find suppliers whose stock is more than demand.

with tmp3 as (
    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
    from v_lineitem
    inner join supplier on l_suppkey = s_suppkey
    inner join nation on s_nationkey = n_nationkey
    inner join part on l_partkey = p_partkey
    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
    and n_name = 'CANADA'
    and p_name like 'forest%'
    group by l_partkey, l_suppkey
)

select
    s_name,
    s_address
from
    v_partsupp
    inner join supplier on ps_suppkey = s_suppkey
    inner join tmp3 on ps_partkey = l_partkey and ps_suppkey = l_suppkey
where
    ps_availqty > sum_quantity
group by
    s_name, s_address
order by
    s_name;
