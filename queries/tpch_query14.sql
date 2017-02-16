-- This query monitors the market response to a promotion such as TV advertisements or a special campaign.
-- Sum saleprice from lineitem, filter by shipdate

with total as (
    select
	    sum(l_saleprice) as total_saleprice
    from
	    v_lineitem 
	    inner join part on l_partkey = p_partkey
    where
        l_shipdate >= '1995-08-01'
	    and l_shipdate < '1995-09-01'
),
promo as (
    select
	    sum(l_saleprice) as promo_saleprice
    from
	    v_lineitem 
	    inner join part on l_partkey = p_partkey
    where
        l_shipdate >= '1995-08-01'
	    and l_shipdate < '1995-09-01'
	    and p_type like 'PROMO%'
)

select 100.00 * promo_saleprice / total_saleprice from promo,total;
