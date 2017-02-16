--  Sum volume and cost, Calculate profit, filter by part name, group by nation name and order year

select
	nation,
	o_year,
	sum(volume) - sum(cost) as sum_profit
from
	(
		select
			n_name as nation,
			o_orderyear as o_year,
			l_saleprice as volume,
			l_supplycost as cost
		from
			v_lineitem
			inner join part on l_partkey = p_partkey
			inner join supplier on l_suppkey = s_suppkey
			inner join v_partsupp on l_suppkey = ps_suppkey and l_partkey = ps_partkey
			inner join v_orders on l_orderkey = o_orderkey
			inner join nation on s_nationkey = n_nationkey
		where
			p_name like '%plum%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
