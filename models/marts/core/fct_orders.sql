WITH orders as (
    select * from {{ ref('stg_orders')}}
),

    payments as (
    select * from {{ ref('stg_payments')}}
    ),

    order_payments as (
        select
        order_id,
        sum(case when status='success' then amount end) as amount
        from payments
        group by 1
    ),

    final as (
        SELECT a.order_id,
        a.customer_id,
        a.order_date,
        coalesce(b.amount, 0) as amount
        FROM orders a
        LEFT JOIN order_payments b
        USING (order_id)
    )

    SELECT * FROM final

    
