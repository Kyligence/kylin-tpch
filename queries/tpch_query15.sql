--  Find the most profitable supplier within given date range.

with revenue_cached as
(
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        sum(l_saleprice) as total_revenue
    from
        v_lineitem
        inner join supplier on s_suppkey=l_suppkey
    where
        l_shipdate >= '1996-01-01'
        and l_shipdate < '1996-04-01'
    group by s_suppkey,s_name,s_address,s_phone
),
max_revenue_cached as
(
    select
        max(total_revenue) as max_revenue
    from
        revenue_cached
)

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    revenue_cached
    inner join max_revenue_cached on total_revenue = max_revenue
order by s_suppkey;

