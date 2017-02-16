--  Count number of delayed orders, filter by date range, group by orderpriority

select
    o_orderpriority,
    count(*) as order_count
from
    (
        select
            l_orderkey,
            o_orderpriority
        from
            v_lineitem
            inner join v_orders on l_orderkey = o_orderkey
        where
            o_orderdate >= '1996-05-01'
            and o_orderdate < '1996-08-01'
            and l_receiptdelayed = 1
        group by
            l_orderkey,
            o_orderpriority
    ) t
group by
    t.o_orderpriority
order by
    t.o_orderpriority;
