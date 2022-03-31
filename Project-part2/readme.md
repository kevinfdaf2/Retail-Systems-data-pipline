# **Project part 2**

# 1. Create a table called order_products_prior by using the last SQL query you created from the previous assignment. 

```sql
CREATE TABLE order_products_prior WITH (external_location = 's3://imba-kevin0019/features/order_products_prior/', format = 'parquet')
as (SELECT a.*, b.product_id,
b.add_to_cart_order,
b.reordered FROM orders a
JOIN order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior')
```

# 2. Create a SQL query (user_features_1). Based on table orders, for each user, calculate the max order_number, the sum of days_since_prior_order and the average of days_since_prior_order.

```sql
SELECT user_id,
    max(order_number) as max_order_number,# (user_orders)
    sum(days_since_prior_order) as sum_days_since_prior_order,# (user_period)
    avg(days_since_prior_order) as avg_days_since_prior_order # (user_mean_days_since_prior)
FROM "prod"."orders" 
group by user_id
limit 10;
```

# 3. Create a SQL query (user_features_2). Similar to above, based on table order_products_prior, for each user calculate the total number of products, total number of distinct products, and user reorder ratio(number of reordered = 1 divided by number of order_number > 1).

```sql
SELECT user_id,
    count(product_id) as product_id_count, # (user_total_products)
    count(distinct product_id) as product_id_count_distinct, # (user_distinct_products)
    sum(case when reordered = 1 then 1 else 0 end)/
    cast(sum(case when order_number > 1 then 1 else 0 end) as double) as user_reorder_ratio
FROM "prod"."order_products_prior"
group by user_id
limit 10;
```


# 4. Create a SQL query (up_features). Based on table order_products_prior, for each user and product, calculate the total number of orders, minimum order_number, maximum order_number and average add_to_cart_order.

```sql
SELECT user_id,
    product_id,
    count(*) as number_ordered, # (up_orders)
    min(order_number) as min_order_number,# (up_first_orders)
    max(order_number) as max_order_number,# (up_last_orders)
    avg(add_to_cart_order) as avg_add_cart# (up_average_cart_position)
    
FROM "prod"."order_products_prior"
group by user_id, product_id
limit 10;
```


# 5. Create a SQL query (prd_features). Based on table order_products_prior, first write a sql query to calculate the sequence of product purchase for each user, and name it product_seq_time (For example, if a user first time purchase a product A, mark it as 1. If it’s the second time a user purchases a product A, mark it as 2).


```sql
select 
    product_id,
    count(*) as product_count, # (prod_orders)
    sum(reordered) as sum_of_reordered,# (prod_reorders)
    sum(case when product_seq_time = 1 then 1 else 0 end) as first_orders,# (prod_first_orders)
    sum(case when product_seq_time = 2 then 1 else 0 end) as second_orders# (prod_second_orders)

from 
(select *, 
    rank() over (partition by user_id, product_id
                order by order_number) as product_seq_time
from order_products_prior)
group by product_id
limit 10;
```






