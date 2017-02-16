--  Sum discount revenue, filter by discount, quantity and time range

select
    sum(l_extendedprice) - sum(l_saleprice) as revenue
from
    v_lineitem
where
    l_shipdate >= '1993-01-01'
    and l_shipdate < '1994-01-01'
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 25;
