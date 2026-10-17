CREATE TABLE lake.gold.q18_top100 WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity
FROM lake.silver.customer_orders
ORDER BY totalprice DESC, orderdate, orderkey LIMIT 100
