select * 
from prod.orders o left join prod.order_products op 
    on o.order_id = op.order_id
where eval_set = 'prior'
limit 20;