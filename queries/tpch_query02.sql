--  Get min ps_supplycost from v_partsupp. And query some details filter by p_size/p_type/r_name.

with q2_min_ps_supplycost as (
	select
		p_partkey as min_p_partkey,
		min(ps_supplycost) as min_ps_supplycost
	from
		v_partsupp
		inner join part on p_partkey = ps_partkey
		inner join supplier on s_suppkey = ps_suppkey
		inner join nation on s_nationkey = n_nationkey
		inner join region on n_regionkey = r_regionkey
	where
		r_name = 'EUROPE'
	group by
		p_partkey
)
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	v_partsupp
	inner join part on p_partkey = ps_partkey
	inner join supplier on s_suppkey = ps_suppkey
	inner join nation on s_nationkey = n_nationkey
	inner join region on n_regionkey = r_regionkey
	inner join q2_min_ps_supplycost on ps_supplycost = min_ps_supplycost and p_partkey = min_p_partkey
where
	p_size = 37
	and p_type like '%COPPER'
	and r_name = 'EUROPE'	
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

