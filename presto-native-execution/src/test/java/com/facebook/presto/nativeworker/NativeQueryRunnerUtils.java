/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;

public class NativeQueryRunnerUtils
{
    private NativeQueryRunnerUtils() {}

    public static Map<String, String> getNativeWorkerHiveProperties()
    {
        return ImmutableMap.of("hive.parquet.pushdown-filter-enabled", "true",
                "hive.orc-compression-codec", "ZSTD", "hive.storage-format", "DWRF");
    }

    public static Map<String, String> getNativeWorkerIcebergProperties()
    {
        return ImmutableMap.of("iceberg.pushdown-filter-enabled", "true",
                "iceberg.catalog.type", "HIVE");
    }

    public static Map<String, String> getNativeWorkerSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("native-execution-enabled", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("regex-library", "RE2J")
                .put("offset-clause-enabled", "true")
                // By default, Presto will expand some functions into its SQL equivalent (e.g. array_duplicates()).
                // With Velox, we do not want Presto to replace the function with its SQL equivalent.
                // To achieve that, we set inline-sql-functions to false.
                .put("inline-sql-functions", "false")
                .put("use-alternative-function-signatures", "true")
                .build();
    }

    public static Map<String, String> getNativeSidecarProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("coordinator-sidecar-enabled", "true")
                .put("exclude-invalid-worker-session-properties", "true")
                .put("presto.default-namespace", "native.default")
                // inline-sql-functions is overridden to be true in sidecar enabled native clusters.
                .put("inline-sql-functions", "true")
                .build();
    }

    public static Map<String, String> getNativeWorkerTpcdsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("tpcds.use-varchar-type", "true")
                .build();
    }

    /**
     * Creates all tables for local testing, except for bench tables.
     *
     * @param queryRunner
     */
    public static void createAllTables(QueryRunner queryRunner)
    {
        createAllTables(queryRunner, true);
    }

    public static void createAllTables(QueryRunner queryRunner, boolean castDateToVarchar)
    {
        createLineitem(queryRunner, castDateToVarchar);
        createOrders(queryRunner, castDateToVarchar);
        createOrdersEx(queryRunner);
        createOrdersHll(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createBucketedCustomer(queryRunner);
        createCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createTableToTestHiddenColumns(queryRunner);
        createSupplier(queryRunner);
        createEmptyTable(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
    }

    /**
     * Creates all iceberg tables for local testing.
     *
     * @param queryRunner
     */
    public static void createAllIcebergTables(QueryRunner queryRunner)
    {
        createLineitemStandard(queryRunner);
        createOrders(queryRunner);
        createNationWithFormat(queryRunner, ICEBERG_DEFAULT_STORAGE_FORMAT);
        createCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createSupplier(queryRunner);
    }

    public static void createLineitem(QueryRunner queryRunner)
    {
        createLineitem(queryRunner, true);
    }

    public static void createLineitem(QueryRunner queryRunner, boolean castDateToVarchar)
    {
        queryRunner.execute("DROP TABLE IF EXISTS lineitem");
        String shipDate = castDateToVarchar ? "cast(shipdate as varchar) as shipdate" : "shipdate";
        String commitDate = castDateToVarchar ? "cast(commitdate as varchar) as commitdate" : "commitdate";
        String receiptDate = castDateToVarchar ? "cast(receiptdate as varchar) as receiptdate" : "receiptdate";
        queryRunner.execute("CREATE TABLE lineitem AS " +
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "   returnflag, linestatus, " + shipDate + ", " + commitDate + ", " + receiptDate + ", " +
                "   shipinstruct, shipmode, comment, " +
                "   linestatus = 'O' as is_open, returnflag = 'R' as is_returned, " +
                "   cast(tax as real) as tax_as_real, cast(discount as real) as discount_as_real, " +
                "   cast(linenumber as smallint) as linenumber_as_smallint, " +
                "   cast(linenumber as tinyint) as linenumber_as_tinyint " +
                "FROM tpch.tiny.lineitem");
    }

    public static void createLineitemStandard(QueryRunner queryRunner)
    {
        createLineitemStandard(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createLineitemStandard(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "lineitem")) {
            queryRunner.execute(session, "CREATE TABLE lineitem AS " +
                    "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate, " +
                    "   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment " +
                    "FROM tpch.tiny.lineitem");
        }
    }

    public static void createOrders(QueryRunner queryRunner)
    {
        createOrders(queryRunner, true);
    }

    public static void createOrders(QueryRunner queryRunner, boolean castDateToVarchar)
    {
        createOrders(queryRunner.getDefaultSession(), queryRunner, castDateToVarchar);
    }

    public static void createOrders(Session session, QueryRunner queryRunner, boolean castDateToVarchar)
    {
        queryRunner.execute(session, "DROP TABLE IF EXISTS orders");
        String orderDate = castDateToVarchar ? "cast(orderdate as varchar) as orderdate" : "orderdate";
        queryRunner.execute(session, "CREATE TABLE orders AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, " + orderDate + ", " +
                "   orderpriority, clerk, shippriority, comment " +
                "FROM tpch.tiny.orders");
    }

    public static void createOrdersEx(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_ex")) {
            queryRunner.execute("CREATE TABLE orders_ex AS " +
                    "SELECT orderkey, array_agg(quantity) as quantities, map_agg(linenumber, quantity) as quantity_by_linenumber " +
                    "FROM tpch.tiny.lineitem " +
                    "GROUP BY 1");
        }
    }

    public static void createOrdersHll(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_hll")) {
            queryRunner.execute("CREATE TABLE orders_hll AS " +
                    "SELECT orderkey % 23 as key, cast(approx_set(cast(orderdate as varchar)) as varbinary) as hll " +
                    "FROM tpch.tiny.orders " +
                    "GROUP BY 1");
        }
    }

    public static void createNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation")) {
            queryRunner.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        }
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation")) {
            queryRunner.execute("CREATE TABLE nation WITH (FORMAT = 'ORC') AS SELECT * FROM tpch.tiny.nation");
        }
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_json")) {
            queryRunner.execute("CREATE TABLE nation_json WITH (FORMAT = 'JSON') AS SELECT * FROM tpch.tiny.nation");
        }
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_text")) {
            queryRunner.execute("CREATE TABLE nation_text WITH (FORMAT = 'TEXTFILE') AS SELECT * FROM tpch.tiny.nation");
        }
    }

    public static void createNationWithFormat(QueryRunner queryRunner, String storageFormat)
    {
        createNationWithFormat(queryRunner.getDefaultSession(), queryRunner, storageFormat);
    }

    public static void createNationWithFormat(Session session, QueryRunner queryRunner, String storageFormat)
    {
        queryRunner.execute(session, "DROP TABLE IF EXISTS nation");
        if (storageFormat.equals("PARQUET") && !queryRunner.tableExists(session, "nation")) {
            queryRunner.execute(session, "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        }

        if (storageFormat.equals("ORC") && !queryRunner.tableExists(session, "nation")) {
            queryRunner.execute(session, "CREATE TABLE nation WITH (FORMAT = 'ORC') AS SELECT * FROM tpch.tiny.nation");
        }

        if (storageFormat.equals("JSON") && !queryRunner.tableExists(session, "nation_json")) {
            queryRunner.execute(session, "CREATE TABLE nation_json WITH (FORMAT = 'JSON') AS SELECT * FROM tpch.tiny.nation");
        }

        if (storageFormat.equals("TEXTFILE") && !queryRunner.tableExists(session, "nation_text")) {
            queryRunner.execute(session, "CREATE TABLE nation_text WITH (FORMAT = 'TEXTFILE') AS SELECT * FROM tpch.tiny.nation");
        }
    }

    public static void createPartitionedNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_partitioned")) {
            queryRunner.execute("CREATE TABLE nation_partitioned(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey VARCHAR) WITH (partitioned_by = ARRAY['regionkey'])");
            queryRunner.execute("INSERT INTO nation_partitioned SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM tpch.tiny.nation");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_partitioned_ds")) {
            queryRunner.execute("CREATE TABLE nation_partitioned_ds(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey VARCHAR, ds VARCHAR) WITH (partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO nation_partitioned_ds SELECT nationkey, name, comment, cast(regionkey as VARCHAR), '2022-04-09' FROM tpch.tiny.nation");
            queryRunner.execute("INSERT INTO nation_partitioned_ds SELECT nationkey, name, comment, cast(regionkey as VARCHAR), '2022-03-18' FROM tpch.tiny.nation");
        }
    }

    public static void createBucketedCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "customer_bucketed")) {
            queryRunner.execute("CREATE TABLE customer_bucketed(acctbal DOUBLE, custkey BIGINT, name VARCHAR, ds VARCHAR) WITH (bucket_count = 10, bucketed_by = ARRAY['name'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-01' FROM tpch.tiny.customer limit 10");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-02' FROM tpch.tiny.customer limit 20");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-03' FROM tpch.tiny.customer limit 30");
        }
    }

    // Create PrestoBench tables that can be useful for Velox testing. PrestoBench leverages TPC-H data generator but it is
    // more representative of modern data warehouses workloads than TPC-H. Main highlights:
    // - It is based on 4 tables which are a denormalized version of the 7 tables in TPC-H.
    // - It supports complex types like arrays and maps which is not covered by TPC-H.
    // - TPC-H data model does not have nulls and this gap is covered by PrestoBench.
    // - Add partitioning and bucketing to some of the tables in PrestoBench.
    public static void createPrestoBenchTables(QueryRunner queryRunner)
    {
        // Create PrestoBench 4 tables
        createPrestoBenchNation(queryRunner);
        createPrestoBenchPart(queryRunner);
        createPrestoBenchCustomer(queryRunner);
        createPrestoBenchOrders(queryRunner);
    }

    public static void createCustomer(QueryRunner queryRunner)
    {
        createCustomer(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createCustomer(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "customer")) {
            queryRunner.execute(session, "CREATE TABLE customer AS " +
                    "SELECT custkey, name, address, nationkey, phone, acctbal, comment, mktsegment " +
                    "FROM tpch.tiny.customer");
        }
    }

    // prestobench_nation: TPC-H Nation and region tables are consolidated into the nation table adding the region name as a new field.
    // This table is not bucketed or partitioned.
    public static void createPrestoBenchNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_nation")) {
            queryRunner.execute("CREATE TABLE prestobench_nation as SELECT nation.nationkey, nation.name, region.name as regionname,nation.comment " +
                    "FROM tpch.tiny.nation, tpch.tiny.region  WHERE nation.regionkey = region.regionkey");
        }
    }

    // prestobench_part: TPC-H part, supplier and part supplier are consolidated into the part table.
    // - Complex types:
    //   * The list of suppliers of each part is captured into an array of JSON objects(about 4 suppliers for each part).
    //     Each JSON has a key and a value corresponding to a supplier.
    //        - The key is the supplier key (suppkey)
    //        - The value is simply the original supplier columns in tpc-h which are: suppkey, name, address, nationkey, phone, acctbal, comment
    // - Partitioning: p_size (50 values)
    // - Bucketing:none to exercise non bucketed joins
    public static void createPrestoBenchPart(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_part")) {
            queryRunner.execute("CREATE TABLE prestobench_part " +
                    "with (partitioned_by = array['size']) " +
                    "as WITH part_suppliers as (SELECT part.partkey, supplier.suppkey, " +
                    "                                  array_agg(cast(row(supplier.suppkey, supplier.name, availqty, supplycost, address, nationkey, phone, acctbal) as " +
                    "                                                 row(suppkey integer, suppname varchar(25), availqty double, suppcost double, address varchar(40), " +
                    "                                                     nationkey integer, phone varchar(15), acctbal double))) suppliers " +
                    "                            FROM tpch.tiny.part part, tpch.tiny.supplier supplier, tpch.tiny.partsupp partsupp " +
                    "                            WHERE supplier.suppkey = partsupp.suppkey and partsupp.partkey = part.partkey GROUP BY 1, 2 " +
                    "                          ), " +
                    "                          part_agg_suppliers as (SELECT partkey, map_agg(suppkey, suppliers) suppliers FROM part_suppliers GROUP BY partkey) " +
                    "SELECT part.partkey, part.name, part.mfgr, part.brand, part.type, part.container, part.retailprice, part.comment, suppliers, part.size " +
                    "FROM tpch.tiny.part part, part_agg_suppliers " +
                    "WHERE part_agg_suppliers.partkey = part.partkey");
        }
    }

    // prestobench_orders: orders and line items are merged to form orders tables.
    // Complex types: The new orders table has all the line items as a map of
    //      * key = line item numbers
    //      * value  = ROW of partkey, suppkey, linenumber, quantity, extendedprice, discount, tax,
    //                        returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode
    // Bucketing: The new order table is bucketed by customer key to help joins with customer table
    // Partitioning: Order date is used commonly in the workload below and is a good candidate for partitioning field.
    //               However, that will produce too many partitions. An alternative is to partition by
    //               year of order date (7 values) and this means we need a field for that since Presto only allows partitioning  by fields.
    // Nulls: Make 10% of custkey as nulls. This is useful for join keys with nulls.
    // Skew: There are already columns with few values like order status that can be used for skewed (hot task) aggregations.
    //       There are three values with these distributions:  ‘F’ with 49%, ‘O’ with 49% and ‘P’ with 2%
    public static void createPrestoBenchOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_orders")) {
            queryRunner.execute("CREATE TABLE prestobench_orders " +
                    "with (partitioned_by = array['orderdate_year'], bucketed_by = array['custkey'], bucket_count = 16) " +
                    "as WITH order_lineitems as (SELECT o.orderkey, l.linenumber, " +
                    "                                   array_agg(cast(row(partkey, suppkey, quantity, extendedprice, discount, tax, returnflag, linestatus, " +
                    "                                                      shipdate, commitdate, receiptdate, shipinstruct, shipmode ) as " +
                    "                                                  row(partkey integer, suppkey integer, quantity integer, extendedprice double, discount double, " +
                    "                                                      tax double , returnflag varchar(1), linestatus varchar(1), " +
                    "                                                      shipdate varchar, commitdate varchar, receiptdate varchar, shipinstruct varchar(25) , shipmode varchar(10)) " +
                    "                                             ) " +
                    "                                            ) lineitems " +
                    "                            FROM orders o, lineitem l " +
                    "                            WHERE o.orderkey = l.orderkey " +
                    "                            GROUP BY 1, 2 " +
                    "                           ), " +
                    "        order_lineitems_agg as (SELECT orderkey, map_agg(linenumber, lineitems) lineitems_map FROM order_lineitems GROUP BY orderkey) " +
                    "SELECT o.orderkey, if(custkey % 10 = 0,null,custkey) as custkey,cast(orderstatus as varchar(1)) as orderstatus," +
                    "       totalprice,cast(orderpriority as varchar(15)) as orderpriority," +
                    "       cast(clerk as varchar(15)) as clerk,shippriority,cast(comment as varchar(79)) as comment,lineitems_map, " +
                    "       cast(orderdate as varchar) orderdate, cast(substr(cast(orderdate as varchar),1,4) as integer) orderdate_year " +
                    "FROM orders o, order_lineitems_agg " +
                    "WHERE order_lineitems_agg.orderkey = o.orderkey");
        }
    }

    // prestobench_customer represents the original tpc-h customer table with additional fields with complex types.
    // Complex Types: Add two fields as map
    //  * key=year of ship date and value is an array of parts info for the customer orders in the year
    //  * key=year of ship date and values is total spending by the customer on orders for the year
    // Bucketing: customer key
    // Partitioning: mktsegment (150 values)
    public static void createPrestoBenchCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_customer")) {
            queryRunner.execute("CREATE TABLE prestobench_customer " +
                    "with (partitioned_by = array['mktsegment'], bucketed_by = array['custkey'], bucket_count = 64) " +
                    "as WITH customer_yearly_summary as (SELECT custkey, map_agg(y, parts) AS year_parts, map_agg(y, total_cost) AS year_cost " +
                    "                                    FROM (SELECT c.custkey,  cast(substr(cast(shipdate as varchar),1,4) as integer) y, " +
                    "                                                 ARRAY_AGG( CAST(ROW (partkey, extendedprice, quantity) AS " +
                    "                                                                 ROW (pk BIGINT, ep DOUBLE, qt DOUBLE))) AS parts, " +
                    "                                                 SUM(extendedprice) AS total_cost " +
                    "                                          FROM customer c LEFT OUTER JOIN orders o ON o.custkey = c.custkey " +
                    "                                               LEFT OUTER JOIN lineitem l " +
                    "                                               ON l.orderkey = o.orderkey " +
                    "                                               GROUP BY c.custkey, cast(substr(cast(shipdate as varchar),1,4) as integer) " +
                    "                                         ) GROUP BY custkey" +
                    "                                   ) " +
                    "SELECT customer.custkey, cast(name as varchar) as name, " +
                    "       cast(address as varchar) as address, nationkey, " +
                    "       cast(phone as varchar) as phone, acctbal, " +
                    "       cast(comment as varchar) as comment, year_parts, year_cost, " +
                    "       cast(mktsegment as varchar) as mktsegment " +
                    "FROM customer, customer_yearly_summary " +
                    "WHERE customer_yearly_summary.custkey=customer.custkey");
        }
    }

    public static void createPart(QueryRunner queryRunner)
    {
        createPart(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createPart(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "part")) {
            queryRunner.execute(session, "CREATE TABLE part AS SELECT * FROM tpch.tiny.part");
        }
    }

    public static void createPartSupp(QueryRunner queryRunner)
    {
        createPartSupp(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createPartSupp(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "partsupp")) {
            queryRunner.execute(session, "CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp");
        }
    }

    public static void createRegion(QueryRunner queryRunner)
    {
        createRegion(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createRegion(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "region")) {
            queryRunner.execute(session, "CREATE TABLE region AS SELECT * FROM tpch.tiny.region");
        }
    }

    public static void createTableToTestHiddenColumns(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "test_hidden_columns")) {
            queryRunner.execute("CREATE TABLE test_hidden_columns (regionkey bigint, name varchar(25), comment varchar(152))");

            // Inserting two rows with 2 seconds delay to have a different modified timestamp for each file.
            queryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM region where regionkey = 0");
            try {
                TimeUnit.SECONDS.sleep(2);
            }
            catch (InterruptedException e) {
            }
            queryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM region where regionkey = 1");
        }
    }

    public static void createSupplier(QueryRunner queryRunner)
    {
        createSupplier(queryRunner.getDefaultSession(), queryRunner);
    }

    public static void createSupplier(Session session, QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(session, "supplier")) {
            queryRunner.execute(session, "CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier");
        }
    }

    public static void createEmptyTable(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "empty_table")) {
            queryRunner.execute("CREATE TABLE empty_table (orderkey BIGINT, shipmodes array(varchar))");
        }
    }

    // Create two bucketed by 'orderkey' tables to be able to run bucketed execution join query on them.
    public static void createBucketedLineitemAndOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem_bucketed")) {
            queryRunner.execute("CREATE TABLE lineitem_bucketed(orderkey BIGINT, partkey BIGINT, suppkey BIGINT, linenumber INTEGER, quantity DOUBLE, ds VARCHAR) " +
                    "WITH (bucket_count = 10, bucketed_by = ARRAY['orderkey'], sorted_by = ARRAY['orderkey'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO lineitem_bucketed SELECT orderkey, partkey, suppkey, linenumber, quantity, '2021-12-20' FROM tpch.tiny.lineitem");
            queryRunner.execute("INSERT INTO lineitem_bucketed SELECT orderkey, partkey, suppkey, linenumber, quantity+10, '2021-12-21' FROM tpch.tiny.lineitem");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_bucketed")) {
            queryRunner.execute("CREATE TABLE orders_bucketed (orderkey BIGINT, custkey BIGINT, orderstatus VARCHAR, ds VARCHAR) " +
                    "WITH (bucket_count = 10, bucketed_by = ARRAY['orderkey'], sorted_by = ARRAY['orderkey'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO orders_bucketed SELECT orderkey, custkey, orderstatus, '2021-12-20' FROM tpch.tiny.orders");
            queryRunner.execute("INSERT INTO orders_bucketed SELECT orderkey, custkey, orderstatus, '2021-12-21' FROM tpch.tiny.orders");
        }
    }
}
