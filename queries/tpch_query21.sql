-- This query identifies certain suppliers who were not able to ship required parts in a timely manner.
-- Count distinct suppkey from lineitem, filter by order status, receiptdeplayed, nation name

select s_name, count(*) as numwait
from
(
    select
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
    from
        v_lineitem l1
        inner join v_orders on l1.l_orderkey = o_orderkey
        inner join supplier on l1.l_suppkey = s_suppkey
        inner join nation on s_nationkey = n_nationkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                v_lineitem inner join v_orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
            group by
                l_orderkey
            having
                count (distinct l_suppkey) > 1
        ) l2 on l1.l_orderkey = l2.l_orderkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                v_lineitem inner join v_orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
                and l_receiptdelayed = 1
            group by
                l_orderkey
            having
                count (distinct l_suppkey) = 1
        ) l3 on l1.l_orderkey = l3.l_orderkey
    where
        o_orderstatus = 'F'
        and l_receiptdelayed = 1
        and n_name = 'SAUDI ARABIA'
    group by
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
)
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;
