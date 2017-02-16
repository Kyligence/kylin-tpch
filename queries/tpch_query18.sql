--  The Large Volume Customer Query finds a list of the top 100 customers who have ever placed large quantity orders. 
--  The query lists the customer name, customer key, the order key, date and total price and the quantity for the order.
--
--  Sum quantity from lineitem, filter by sum(quantity), group by customer name, custkey, orderkey, orderdate and totalprice

select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    v_lineitem
    inner join v_orders on l_orderkey = o_orderkey
    inner join customer on o_custkey = c_custkey
where
    o_orderkey is not null
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
having
    sum(l_quantity) > 300
order by
    o_totalprice desc,
    o_orderdate 
limit 100;
