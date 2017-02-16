--  Sum revenue, filter by order date range and return flag, group by customer key, name, account balance,
--  phone, address, comment and nation name.

select
	c_custkey,
	c_name,
	sum(l_saleprice) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
    v_lineitem
    inner join v_orders on l_orderkey = o_orderkey
	inner join customer on c_custkey = o_custkey
    inner join nation on c_nationkey = n_nationkey
where
	o_orderdate >= '1993-07-01'
	and o_orderdate < '1993-10-01'
	and l_returnflag = 'R'
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
