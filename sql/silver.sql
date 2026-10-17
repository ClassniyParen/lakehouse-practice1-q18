CREATE TABLE lake.silver.customer WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT custkey, trim(name) AS name FROM lake.bronze.customer
WHERE custkey IS NOT NULL AND name IS NOT NULL;

CREATE TABLE lake.silver.orders WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, custkey, CAST(orderdate AS date) AS orderdate,
       CAST(totalprice AS decimal(15,2)) AS totalprice
FROM lake.bronze.orders
WHERE orderkey IS NOT NULL AND custkey IS NOT NULL
  AND orderdate IS NOT NULL AND totalprice >= 0;

CREATE TABLE lake.silver.lineitem WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, linenumber, CAST(quantity AS decimal(15,2)) AS quantity
FROM lake.bronze.lineitem
WHERE orderkey IS NOT NULL AND linenumber IS NOT NULL AND quantity > 0;

CREATE TABLE lake.silver.large_orders WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, sum(quantity) AS total_quantity
FROM lake.silver.lineitem
GROUP BY orderkey HAVING sum(quantity) > 300;

CREATE TABLE lake.silver.customer_orders WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, l.total_quantity
FROM lake.silver.large_orders l
JOIN lake.silver.orders o ON l.orderkey = o.orderkey
JOIN lake.silver.customer c ON o.custkey = c.custkey;
