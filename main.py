import pyarrow
import datafusion

if __name__ == "__main__":
    ctx = datafusion.ExecutionContext()
    ctx.register_parquet("lineitem", "/tmp/tpch_sf1/lineitem/lineitem.parquet")
    ctx.register_parquet("orders", "/tmp/tpch_sf1/orders/orders.parquet")
    ctx.register_parquet("customer", "/tmp/tpch_sf1/customer/customer.parquet")
    ctx.register_parquet("partsupp", "/tmp/tpch_sf1/partsupp/partsupp.parquet")
    ctx.register_parquet("part", "/tmp/tpch_sf1/part/part.parquet")
    ctx.register_parquet("nation", "/tmp/tpch_sf1/nation/nation.parquet")
    ctx.register_parquet("region", "/tmp/tpch_sf1/region/region.parquet")
    ctx.register_parquet("supplier", "/tmp/tpch_sf1/supplier/supploer.parquet")


    query = """
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
    """

    batches = ctx.sql(query).collect()
    print(batches)
