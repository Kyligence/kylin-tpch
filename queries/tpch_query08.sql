-- Calculate mkt share percent(sum saleprice) of some nation from lineitem, filter by region name, order date range, nation name
-- group by order year

with all_nations as (
    select
			o_orderyear as o_year,
			l_saleprice as volume,
			n2.n_name as nation
		from
		    v_lineitem
		    inner join part on l_partkey = p_partkey
		    inner join supplier on l_suppkey = s_suppkey
			inner join v_orders on l_orderkey = o_orderkey
			inner join customer on o_custkey = c_custkey
		    inner join nation n1 on c_nationkey = n1.n_nationkey
		    inner join nation n2 on s_nationkey = n2.n_nationkey
		    inner join region on n1.n_regionkey = r_regionkey
		where
			r_name = 'AMERICA'
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY BURNISHED NICKEL'
),
peru as (
    select o_year, sum(volume) as peru_volume from all_nations where nation = 'PERU' group by o_year
),
all_data as (
    select o_year, sum(volume) as all_volume from all_nations group by o_year
)
select peru.o_year, peru_volume / all_volume as mkt_share from peru inner join all_data on peru.o_year = all_data.o_year;
