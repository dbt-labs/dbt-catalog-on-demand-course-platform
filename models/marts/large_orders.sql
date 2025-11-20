with total_order_revenue as (
    select * from {{ ref('catalog_finance', 'total_order_revenue') }}
),

final as (
    select *,
    case 
        when order_revenue > 1000 then 'large_order'
        else 'normal_order' 
    end as order_size
    from total_order_revenue
)

select * from final