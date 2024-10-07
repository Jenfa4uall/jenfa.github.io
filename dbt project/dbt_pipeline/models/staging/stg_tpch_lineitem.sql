SELECT
    {{ dbt_utils.generate_surrogate_key(['L_ORDERKEY', 'L_LINENUMBER']) }} AS order_item_key,
    L_ORDERKEY AS order_key,
    L_PARTKEY AS part_key,
    L_LINENUMBER AS line_number,
    L_QUANTITY AS quantity,
    L_EXTENDEDPRICE AS extended_price,
    L_DISCOUNT AS discount_percentage,
    L_TAX AS tax_rate
FROM 
    {{ source('tpch', 'LINEITEM') }}
