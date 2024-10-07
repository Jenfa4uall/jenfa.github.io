select 
O_ORDERKEY as order_key,
O_CUSTKEY as customer_key,
O_ORDERSTATUS as status_code,
O_TOTALPRICE as total_price,
O_ORDERDATE as order_date
from {{ source('tpch','orders') }}