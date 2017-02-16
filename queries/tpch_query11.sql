-- This query finds the most important subset of suppliers' stock in a given nation.
-- Sum part value from partsupp, filter by nation name, part_value, group by partkey

with q11_part_tmp_cached as (
	select
		ps_partkey,
		sum(ps_partvalue) as part_value
	from
		v_partsupp
		inner join supplier on ps_suppkey = s_suppkey
		inner join nation on s_nationkey = n_nationkey
	where
		n_name = 'GERMANY'
	group by ps_partkey
),
q11_sum_tmp_cached as (
	select
		sum(part_value) as total_value
	from
		q11_part_tmp_cached
)

select
	ps_partkey, 
	part_value
from (
	select
		ps_partkey,
		part_value,
		total_value
	from
		q11_part_tmp_cached, q11_sum_tmp_cached
) a
where
	part_value > total_value * 0.0001
order by
	part_value desc;
