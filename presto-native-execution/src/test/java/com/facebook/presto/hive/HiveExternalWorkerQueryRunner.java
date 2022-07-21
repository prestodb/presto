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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class HiveExternalWorkerQueryRunner
{
    private HiveExternalWorkerQueryRunner() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getenv("PRESTO_SERVER");
        String baseDataDir = System.getenv("DATA_DIR");
        String workerCount = System.getenv("WORKER_COUNT");
        int cacheMaxSize = 4096; // 4GB size cache

        return createQueryRunner(
                Optional.ofNullable(prestoServerPath),
                Optional.ofNullable(baseDataDir).map(Paths::get),
                Optional.ofNullable(workerCount).map(Integer::parseInt),
                cacheMaxSize);
    }

    public static QueryRunner createQueryRunner(
            Optional<String> prestoServerPath,
            Optional<Path> baseDataDir,
            Optional<Integer> workerCount,
            int cacheMaxSize)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(baseDataDir.isPresent(), "Path to data files must be specified when testing external workers");
        }

        QueryRunner defaultQueryRunner = createJavaQueryRunner(baseDataDir);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        return createNativeQueryRunner(baseDataDir.get().toString(), prestoServerPath.get(), workerCount, cacheMaxSize, true);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> baseDataDir)
            throws Exception
    {
        DistributedQueryRunner queryRunner =
                HiveQueryRunner.createQueryRunner(
                        ImmutableList.of(),
                        ImmutableMap.of(
                                "parse-decimal-literals-as-double", "true",
                                "regex-library", "RE2J",
                                "offset-clause-enabled", "true",
                                "deprecated.legacy-date-timestamp-to-varchar-coercion", "true"),
                        "sql-standard",
                        ImmutableMap.of(
                                "hive.storage-format", "DWRF",
                                "hive.pushdown-filter-enabled", "true"),
                        baseDataDir);

        // DWRF doesn't support date type. Convert date columns to varchar for lineitem and orders.
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createOrdersHll(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createBucketedCustomer(queryRunner);
        createCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createSupplier(queryRunner);
        createEmptyTable(queryRunner);
        createPrestoBenchTables(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);

        return queryRunner;
    }

    public static QueryRunner createNativeQueryRunner(String baseDataDir, String prestoServerPath, Optional<Integer> workerCount, int cacheMaxSize, boolean useThrift)
            throws Exception
    {
        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("optimizer.optimize-hash-generation", "false")
                        .put("parse-decimal-literals-as-double", "true")
                        .put("http-server.http.port", "8080")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .put("regex-library", "RE2J")
                        .put("offset-clause-enabled", "true")
                        .put("deprecated.legacy-date-timestamp-to-varchar-coercion", "true")
                        // By default, Presto will expand some functions into its SQL equivalent (e.g. array_duplicates()).
                        // With Velox, we do not want Presto to replace the function with its SQL equivalent.
                        // To achieve that, we set inline-sql-functions to false.
                        .put("inline-sql-functions", "false")
                        .put("use-alternative-function-signatures", "true")
                        .build(),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of(
                        "hive.storage-format", "DWRF",
                        "hive.pushdown-filter-enabled", "true"),
                workerCount,
                Optional.of(Paths.get(baseDataDir)),
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(HiveExternalWorkerQueryRunner.class.getSimpleName());
                        Logger log = Logger.get(HiveExternalWorkerQueryRunner.class);
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("config.properties"),
                                format("discovery.uri=%s\n" +
                                        "presto.version=testversion\n" +
                                        "http_exec_threads=8\n" +
                                        "system-memory-gb=4\n" +
                                        "http-server.http.port=%d", discoveryUri, port).getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s\n" +
                                        "node.ip=127.0.0.1\n" +
                                        "node.environment=testing\n" +
                                        "node.location=test-location", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        if (cacheMaxSize > 0) {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive\n" +
                                            "cache.enabled=true\n" +
                                            "cache.max-cache-size=%s", cacheMaxSize).getBytes());
                        }
                        else {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive").getBytes());
                        }

                        // Disable stack trace capturing as some queries (using TRY) generate a lot of exceptions.
                        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".err").toFile()))
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
    }

    private static void createLineitem(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem")) {
            queryRunner.execute("CREATE TABLE lineitem AS " +
                    "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate, " +
                    "   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment, " +
                    "   linestatus = 'O' as is_open, returnflag = 'R' as is_returned, " +
                    "   cast(tax as real) as tax_as_real, cast(discount as real) as discount_as_real, " +
                    "   cast(linenumber as smallint) as linenumber_as_smallint, " +
                    "   cast(linenumber as tinyint) as linenumber_as_tinyint " +
                    "FROM tpch.tiny.lineitem");
        }
    }

    private static void createOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders")) {
            queryRunner.execute("CREATE TABLE orders AS " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, cast(orderdate as varchar) as orderdate, " +
                    "   orderpriority, clerk, shippriority, comment " +
                    "FROM tpch.tiny.orders");
        }
    }

    private static void createOrdersEx(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_ex")) {
            queryRunner.execute("CREATE TABLE orders_ex AS " +
                    "SELECT orderkey, array_agg(quantity) as quantities, map_agg(linenumber, quantity) as quantity_by_linenumber " +
                    "FROM tpch.tiny.lineitem " +
                    "GROUP BY 1");
        }
    }

    private static void createOrdersHll(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_hll")) {
            queryRunner.execute("CREATE TABLE orders_hll AS " +
                    "SELECT orderkey % 23 as key, cast(approx_set(cast(orderdate as varchar)) as varbinary) as hll " +
                    "FROM tpch.tiny.orders " +
                    "GROUP BY 1");
        }
    }

    private static void createNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation")) {
            queryRunner.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        }
    }

    private static void createPartitionedNation(QueryRunner queryRunner)
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

    private static void createBucketedCustomer(QueryRunner queryRunner)
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
    private static void createPrestoBenchTables(QueryRunner queryRunner)
    {
        // Create PrestoBench 4 tables
        createPrestoBenchNation(queryRunner);
        createPrestoBenchPart(queryRunner);
        createPrestoBenchCustomer(queryRunner);
        createPrestoBenchOrders(queryRunner);
    }

    private static void createCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "customer")) {
            queryRunner.execute("CREATE TABLE customer AS " +
                    "SELECT custkey, name, address, nationkey, phone, acctbal, comment, mktsegment " +
                    "FROM tpch.tiny.customer");
        }
    }

    // prestobench_nation: TPC-H Nation and region tables are consolidated into the nation table adding the region name as a new field.
    // This table is not bucketed or partitioned.
    private static void createPrestoBenchNation(QueryRunner queryRunner)
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
    private static void createPrestoBenchPart(QueryRunner queryRunner)
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
    private static void createPrestoBenchOrders(QueryRunner queryRunner)
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
    private static void createPrestoBenchCustomer(QueryRunner queryRunner)
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

    private static void createPart(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "part")) {
            queryRunner.execute("CREATE TABLE part AS SELECT * FROM tpch.tiny.part");
        }
    }

    private static void createPartSupp(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "partsupp")) {
            queryRunner.execute("CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp");
        }
    }

    private static void createRegion(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "region")) {
            queryRunner.execute("CREATE TABLE region AS SELECT * FROM tpch.tiny.region");
        }
    }

    private static void createSupplier(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "supplier")) {
            queryRunner.execute("CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier");
        }
    }

    private static void createEmptyTable(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "empty_table")) {
            queryRunner.execute("CREATE TABLE empty_table (orderkey BIGINT, shipmodes array(varchar))");
        }
    }

    // Create two bucketed by 'orderkey' tables to be able to run bucketed execution join query on them.
    private static void createBucketedLineitemAndOrders(QueryRunner queryRunner)
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

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work
        Logging.initialize();

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) HiveExternalWorkerQueryRunner.createQueryRunner();
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
