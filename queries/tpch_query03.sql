--  Sum revenue from lineitems, filter by mktsegment and time condition.

select
    l_orderkey,
    sum(l_saleprice) as revenue,
    o_orderdate,
    o_shippriority
from
    v_lineitem
    inner join v_orders on l_orderkey = o_orderkey
    inner join customer on c_custkey = o_custkey
where
    c_mktsegment = 'BUILDING'
    and o_orderdate < '1995-03-22'
    and l_shipdate > '1995-03-22'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
