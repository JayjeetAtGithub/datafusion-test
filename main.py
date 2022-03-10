import os
import pyarrow
import datafusion

if __name__ == "__main__":
    ctx = datafusion.ExecutionContext()
    ctx.register_parquet("lineitem", "/tmp/tpch_sf1_parquet/lineitem/lineitem.tbl.parquet")
    ctx.register_parquet("orders", "/tmp/tpch_sf1_parquet/orders/orders.tbl.parquet")
    ctx.register_parquet("customer", "/tmp/tpch_sf1_parquet/customer/customer.tbl.parquet")
    ctx.register_parquet("partsupp", "/tmp/tpch_sf1_parquet/partsupp/partsupp.tbl.parquet")
    ctx.register_parquet("part", "/tmp/tpch_sf1_parquet/part/part.tbl.parquet")
    ctx.register_parquet("nation", "/tmp/tpch_sf1_parquet/nation/nation.tbl.parquet")
    ctx.register_parquet("region", "/tmp/tpch_sf1_parquet/region/region.tbl.parquet")
    ctx.register_parquet("supplier", "/tmp/tpch_sf1_parquet/supplier/supplier.tbl.parquet")



    querie_files = os.listdir("queries")
    for query_file in querie_files:
        with open("queries/" + query_file, "r") as f:
            query = f.read()
        
        try:
            print("Executing query: " + query_file)
            result = ctx.sql(query).collect()
            print(result)
            print("\n")
        except Exception as e:
            print("Error: " + str(e))
            print("\n")
