use tpch_flat_orc_;

drop view v_lineitem;

create view if not exists v_lineitem as
select
    lineitem.*,

    year(l_shipdate) as l_shipyear,
    case when l_commitdate < l_receiptdate then 1 else 0 end as l_receiptdelayed,
    case when l_shipdate < l_commitdate then 0 else 1 end as l_shipdelayed,
    
    l_extendedprice * (1 - l_discount) as l_saleprice,
    l_extendedprice * (1 - l_discount) * l_tax as l_taxprice,
    ps_supplycost * l_quantity as l_supplycost
from
    lineitem
    inner join partsupp on l_partkey=ps_partkey and l_suppkey=ps_suppkey
;


drop view v_orders;

create view if not exists v_orders as
select
    orders.*,
    year(o_orderdate) as o_orderyear
from
    orders
;

drop view v_partsupp;
create view if not exists v_partsupp as
select
    partsupp.*,
    ps_supplycost * ps_availqty as ps_partvalue
from
    partsupp
;
