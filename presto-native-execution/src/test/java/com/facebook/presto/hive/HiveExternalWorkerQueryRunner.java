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
import com.facebook.presto.Session;
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
    public static final String DEFAULT_STORAGE_FORMAT = "PARQUET";

    private HiveExternalWorkerQueryRunner() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getenv("PRESTO_SERVER");
        String dataDirectory = System.getenv("DATA_DIR");
        String workerCount = System.getenv("WORKER_COUNT");
        int cacheMaxSize = 4096; // 4GB size cache

        checkArgument(prestoServerPath != null, "Native worker binary path is missing. Add PRESTO_SERVER environment variable.");
        checkArgument(dataDirectory != null, "Data directory path is missing.. Add DATA_DIR environment variable.");

        return createQueryRunner(
                Optional.ofNullable(prestoServerPath),
                Optional.ofNullable(dataDirectory).map(Paths::get),
                Optional.ofNullable(workerCount).map(Integer::parseInt),
                cacheMaxSize);
    }

    public static QueryRunner createQueryRunner(
            Optional<String> prestoServerPath,
            Optional<Path> dataDirectory,
            Optional<Integer> workerCount,
            int cacheMaxSize)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(dataDirectory.isPresent(), "Path to data files must be specified when testing external workers");
        }

        QueryRunner defaultQueryRunner = createJavaQueryRunner(dataDirectory, DEFAULT_STORAGE_FORMAT);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        return createNativeQueryRunner(dataDirectory.get().toString(), prestoServerPath.get(), workerCount, cacheMaxSize, true, DEFAULT_STORAGE_FORMAT);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> dataDirectory, String storageFormat)
            throws Exception
    {
        Optional<Path> finalDataDir = dataDirectory.map(path -> Paths.get(path.toString() + '/' + storageFormat));
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
                                "hive.storage-format", storageFormat,
                                "hive.pushdown-filter-enabled", "true"),
                        finalDataDir);

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

        Session hiveTpcdsSession = Session.builder(queryRunner.getDefaultSession())
                .setSchema("tpcds").build();
        createTpcdsCallCenter(queryRunner, hiveTpcdsSession);
        createTpcdsCatalogPage(queryRunner, hiveTpcdsSession);
        createTpcdsCatalogReturns(queryRunner, hiveTpcdsSession);
        createTpcdsCatalogSales(queryRunner, hiveTpcdsSession);
        createTpcdsCustomer(queryRunner, hiveTpcdsSession);
        createTpcdsCustomerAddress(queryRunner, hiveTpcdsSession);
        createTpcdsCustomerDemographics(queryRunner, hiveTpcdsSession);
        createTpcdsDateDim(queryRunner, hiveTpcdsSession);
        createTpcdsHouseholdDemographics(queryRunner, hiveTpcdsSession);
        createTpcdsIncomeBand(queryRunner, hiveTpcdsSession);
        createTpcdsInventory(queryRunner, hiveTpcdsSession);
        createTpcdsItem(queryRunner, hiveTpcdsSession);
        createTpcdsPromotion(queryRunner, hiveTpcdsSession);
        createTpcdsReason(queryRunner, hiveTpcdsSession);
        createTpcdsShipMode(queryRunner, hiveTpcdsSession);
        createTpcdsStore(queryRunner, hiveTpcdsSession);
        createTpcdsStoreReturns(queryRunner, hiveTpcdsSession);
        createTpcdsStoreSales(queryRunner, hiveTpcdsSession);
        createTpcdsTimeDim(queryRunner, hiveTpcdsSession);
        createTpcdsWarehouse(queryRunner, hiveTpcdsSession);
        createTpcdsWebPage(queryRunner, hiveTpcdsSession);
        createTpcdsWebReturns(queryRunner, hiveTpcdsSession);
        createTpcdsWebSales(queryRunner, hiveTpcdsSession);
        createTpcdsWebSite(queryRunner, hiveTpcdsSession);

        return queryRunner;
    }

    public static QueryRunner createNativeQueryRunner(
            String dataDirectory,
            String prestoServerPath,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            boolean useThrift,
            String storageFormat)
            throws Exception
    {
        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("optimizer.optimize-hash-generation", "false")
                        .put("optimizer.use-mark-distinct", "false")
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
                        .put("query.max-stage-count", "150")
                        .build(),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of(
                        "hive.storage-format", storageFormat,
                        "hive.pushdown-filter-enabled", "true",
                        "hive.parquet.pushdown-filter-enabled", "true"),
                workerCount,
                Optional.of(Paths.get(dataDirectory + '/' + storageFormat)),
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(HiveExternalWorkerQueryRunner.class.getSimpleName());
                        Logger log = Logger.get(HiveExternalWorkerQueryRunner.class);
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("config.properties"),
                                format("discovery.uri=%s%n" +
                                        "presto.version=testversion%n" +
                                        "http_exec_threads=8%n" +
                                        "system-memory-gb=4%n" +
                                        "http-server.http.port=%d", discoveryUri, port).getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s%n" +
                                        "node.ip=127.0.0.1%n" +
                                        "node.environment=testing%n" +
                                        "node.location=test-location", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        if (cacheMaxSize > 0) {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive%n" +
                                           "cache.enabled=true%n" +
                                           "cache.max-cache-size=%s", cacheMaxSize).getBytes());
                        }
                        else {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive").getBytes());
                        }
                        // Add a hive catalog with caching always enabled.
                        Files.write(catalogDirectoryPath.resolve("hivecached.properties"),
                                format("connector.name=hive%n" +
                                        "cache.enabled=true%n" +
                                        "cache.max-cache-size=32").getBytes());

                        // Add a tpch catalog.
                        Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                                format("connector.name=tpch%n").getBytes());

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

    private static void createTpcdsCallCenter(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "call_center")) {
            queryRunner.execute(session, "CREATE TABLE call_center AS " +
                    "SELECT cc_call_center_sk, cast(cc_call_center_id as varchar) as cc_call_center_id, cast(cc_rec_start_date as varchar) as cc_rec_start_date, " +
                    "   cast(cc_rec_end_date as varchar) as cc_rec_end_date, " +
                    "   cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cast(cc_hours as varchar) as cc_hours, " +
                    "   cc_manager, cc_mkt_id, cast(cc_mkt_class as varchar) as cc_mkt_class, cc_mkt_desc, cc_market_manager,  " +
                    "   cc_division, cc_division_name, cc_company, cast(cc_company_name as varchar) as cc_company_name," +
                    "   cast(cc_street_number as varchar ) as cc_street_number, cc_street_name, cast(cc_street_type as varchar) as cc_street_type, " +
                    "   cast(cc_suite_number as varchar) as cc_suite_number, cc_city, cc_county, cast(cc_state as varchar) as cc_state, " +
                    "   cast(cc_zip as varchar) as cc_zip, cc_country, cast(cc_gmt_offset as double) as cc_gmt_offset, " +
                    "   cast(cc_tax_percentage as double) as cc_tax_percentage " +
                    "FROM tpcds.tiny.call_center");
        }
    }

    private static void createTpcdsCatalogPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_page")) {
            queryRunner.execute(session, "CREATE TABLE catalog_page AS " +
                    "SELECT cp_catalog_page_sk, cast(cp_catalog_page_id as varchar) as cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, " +
                    "   cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type " +
                    "FROM tpcds.tiny.catalog_page");
        }
    }

    private static void createTpcdsCatalogReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_returns")) {
            queryRunner.execute(session, "CREATE TABLE catalog_returns AS " +
                    "SELECT cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk,  " +
                    "   cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk,  " +
                    "   cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk,  " +
                    "   cr_order_number, cr_return_quantity, cast(cr_return_amount as double) as cr_return_amount, " +
                    "   cast(cr_return_tax as double) as cr_return_tax, cast(cr_return_amt_inc_tax as double) as cr_return_amt_inc_tax, " +
                    "   cast(cr_fee as double) as cr_fee, cast(cr_return_ship_cost as double) as cr_return_ship_cost, " +
                    "   cast(cr_refunded_cash as double) as cr_refunded_cash, cast(cr_reversed_charge as double) as cr_reversed_charge, " +
                    "   cast(cr_store_credit as double) as cr_store_credit, cast(cr_net_loss as double) as cr_net_loss " +
                    "FROM tpcds.tiny.catalog_returns");
        }
    }

    private static void createTpcdsCatalogSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_sales")) {
            queryRunner.execute(session, "CREATE TABLE catalog_sales AS " +
                    "SELECT cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk,  " +
                    "   cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk,  " +
                    "   cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk,  " +
                    "   cs_promo_sk, cs_order_number, cs_quantity, cast(cs_wholesale_cost as double) as cs_wholesale_cost, " +
                    "   cast(cs_list_price as double) as cs_list_price, cast(cs_sales_price as double) as cs_sales_price, " +
                    "   cast(cs_ext_discount_amt as double) as cs_ext_discount_amt, cast(cs_ext_sales_price as double) as cs_ext_sales_price, " +
                    "   cast(cs_ext_wholesale_cost as double) as cs_ext_wholesale_cost, cast(cs_ext_list_price as double) as cs_ext_list_price, " +
                    "   cast(cs_ext_tax as double) as cs_ext_tax, cast(cs_coupon_amt as double) as cs_coupon_amt, " +
                    "   cast(cs_ext_ship_cost as double) as cs_ext_ship_cost, cast(cs_net_paid as double) as cs_net_paid, " +
                    "   cast(cs_net_paid_inc_tax as double) as cs_net_paid_inc_tax, cast(cs_net_paid_inc_ship as double) as cs_net_paid_inc_ship, " +
                    "   cast(cs_net_paid_inc_ship_tax as double) as cs_net_paid_inc_ship_tax, cast(cs_net_profit as double) as cs_net_profit " +
                    "FROM tpcds.tiny.catalog_sales");
        }
    }

    private static void createTpcdsCustomer(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer")) {
            queryRunner.execute(session, "CREATE TABLE customer AS " +
                    "SELECT c_customer_sk, cast(c_customer_id as varchar) as c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk,  " +
                    "   c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, cast(c_salutation as varchar) as c_salutation,  " +
                    "   cast(c_first_name as varchar) as c_first_name, cast(c_last_name as varchar) as c_last_name, " +
                    "   cast(c_preferred_cust_flag as varchar) as c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, " +
                    "   c_birth_country,  cast(c_login as varchar) as c_login, cast(c_email_address as varchar) as c_email_address,  " +
                    "   c_last_review_date_sk " +
                    "FROM tpcds.tiny.customer");
        }
    }

    private static void createTpcdsCustomerAddress(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_address")) {
            queryRunner.execute(session, "CREATE TABLE customer_address AS " +
                    "SELECT ca_address_sk, cast(ca_address_id as varchar) as ca_address_id, cast(ca_street_number as varchar) as ca_street_number,  " +
                    "   ca_street_name, cast(ca_street_type as varchar) as ca_street_type, cast(ca_suite_number as varchar) as ca_suite_number,  " +
                    "   ca_city, ca_county, cast(ca_state as varchar) as ca_state, cast(ca_zip as varchar) as ca_zip, " +
                    "   ca_country, cast(ca_gmt_offset as double) as ca_gmt_offset, " +
                    "   cast(ca_location_type as varchar) as ca_location_type " +
                    "FROM tpcds.tiny.customer_address");
        }
    }

    private static void createTpcdsCustomerDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_demographics")) {
            queryRunner.execute(session, "CREATE TABLE customer_demographics AS " +
                    "SELECT cd_demo_sk, cast(cd_gender as varchar) as cd_gender, cast(cd_marital_status as varchar) as cd_marital_status,  " +
                    "   cast(cd_education_status as varchar) as cd_education_status, cd_purchase_estimate,  " +
                    "   cast(cd_credit_rating as varchar) as cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count " +
                    "FROM tpcds.tiny.customer_demographics");
        }
    }

    private static void createTpcdsDateDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "date_dim")) {
            queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                    "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, cast(d_date as varchar) as d_date, " +
                    "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                    "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                    "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                    "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                    "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                    "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                    "   cast(d_current_year as varchar) as d_current_year " +
                    "FROM tpcds.tiny.date_dim");
        }
    }

    private static void createTpcdsHouseholdDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "household_demographics")) {
            queryRunner.execute(session, "CREATE TABLE household_demographics AS " +
                    "SELECT hd_demo_sk, hd_income_band_sk, cast(hd_buy_potential as varchar) as hd_buy_potential, hd_dep_count, hd_vehicle_count " +
                    "FROM tpcds.tiny.household_demographics");
        }
    }

    private static void createTpcdsIncomeBand(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "income_band")) {
            queryRunner.execute(session, "CREATE TABLE income_band AS " +
                    "SELECT * FROM tpcds.tiny.income_band");
        }
    }

    private static void createTpcdsInventory(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "inventory")) {
            queryRunner.execute(session, "CREATE TABLE inventory AS " +
                    "SELECT * FROM tpcds.tiny.inventory");
        }
    }

    private static void createTpcdsItem(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "item")) {
            queryRunner.execute(session, "CREATE TABLE item AS " +
                    "SELECT i_item_sk, cast(i_item_id as varchar) as i_item_id, cast(i_rec_start_date as varchar) as i_rec_start_date, " +
                    "   cast(i_rec_end_date as varchar) as i_rec_end_date, i_item_desc, cast(i_current_price as double) as i_current_price, " +
                    "   cast(i_wholesale_cost as double) as i_wholesale_cost, i_brand_id, cast(i_brand as varchar) as i_brand, " +
                    "   i_class_id,  cast(i_class as varchar) as i_class, i_category_id, cast(i_category as varchar) as i_category, i_manufact_id, " +
                    "   cast(i_manufact as varchar) as i_manufact, cast(i_size as varchar) as i_size, cast(i_formulation as varchar) as i_formulation, " +
                    "   cast(i_color as varchar) as i_color, cast(i_units as varchar) as i_units, cast(i_container as varchar) as i_container, i_manager_id, " +
                    "   cast(i_product_name as varchar) as i_product_name " +
                    "FROM tpcds.tiny.item");
        }
    }

    private static void createTpcdsPromotion(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "promotion")) {
            queryRunner.execute(session, "CREATE TABLE promotion AS " +
                    "SELECT p_promo_sk, cast(p_promo_id as varchar) as p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, " +
                    "   cast(p_cost as double) as p_cost, p_response_targe, cast(p_promo_name as varchar) as p_promo_name, " +
                    "   cast(p_channel_dmail as varchar) as p_channel_dmail, cast(p_channel_email as varchar) as p_channel_email, " +
                    "   cast(p_channel_catalog as varchar) as p_channel_catalog, cast(p_channel_tv as varchar) as p_channel_tv, " +
                    "   cast(p_channel_radio as varchar) as p_channel_radio, cast(p_channel_press as varchar) as p_channel_press, " +
                    "   cast(p_channel_event as varchar) as p_channel_event, cast(p_channel_demo as varchar) as p_channel_demo, p_channel_details, " +
                    "   cast(p_purpose as varchar) as p_purpose, cast(p_discount_active as varchar) as p_discount_active " +
                    "FROM tpcds.tiny.promotion");
        }
    }

    private static void createTpcdsReason(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "reason")) {
            queryRunner.execute(session, "CREATE TABLE reason AS " +
                    "SELECT r_reason_sk, cast(r_reason_id as varchar) as r_reason_id, cast(r_reason_desc as varchar) as r_reason_desc " +
                    "FROM tpcds.tiny.reason");
        }
    }

    private static void createTpcdsShipMode(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "ship_mode")) {
            queryRunner.execute(session, "CREATE TABLE ship_mode AS " +
                    "SELECT sm_ship_mode_sk, cast(sm_ship_mode_id as varchar) as sm_ship_mode_id, cast(sm_type as varchar) as sm_type, " +
                    "   cast(sm_code as varchar) as sm_code, cast(sm_carrier as varchar) as sm_carrier, cast(sm_contract as varchar) as sm_contract " +
                    "FROM tpcds.tiny.ship_mode");
        }
    }

    private static void createTpcdsStore(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store")) {
            queryRunner.execute(session, "CREATE TABLE store AS " +
                    "SELECT s_store_sk, cast(s_store_id as varchar) as s_store_id, cast(s_rec_start_date as varchar) as s_rec_start_date, " +
                    "   cast(s_rec_end_date as varchar) as s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, " +
                    "   cast(s_hours as varchar) as s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, " +
                    "   s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, " +
                    "   cast(s_street_type as varchar) as s_street_type, cast(s_suite_number as varchar) as s_suite_number, s_city, s_county, " +
                    "   cast(s_state as varchar ) as s_state, cast(s_zip as varchar) as s_zip, s_country, " +
                    "   cast(s_gmt_offset as double) as s_gmt_offset, " +
                    "   cast(s_tax_precentage as double) as s_tax_precentage " +
                    "FROM tpcds.tiny.store");
        }
    }

    private static void createTpcdsStoreReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_returns")) {
            queryRunner.execute(session, "CREATE TABLE store_returns AS " +
                    "SELECT sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, " +
                    "   sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity,  " +
                    "   cast(sr_return_amt as double) as sr_return_amt, cast(sr_return_tax as double) as sr_return_tax, " +
                    "   cast(sr_return_amt_inc_tax as double) as sr_return_amt_inc_tax,  cast(sr_fee as double) as sr_fee, " +
                    "   cast(sr_return_ship_cost as double) as sr_return_ship_cost, cast(sr_refunded_cash as double) as sr_refunded_cash, " +
                    "   cast(sr_reversed_charge as double) as sr_reversed_charge, cast(sr_store_credit as double) as sr_store_credit, " +
                    "   cast(sr_net_loss as double) as sr_net_loss " +
                    "FROM tpcds.tiny.store_returns");
        }
    }

    private static void createTpcdsStoreSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_sales")) {
            queryRunner.execute(session, "CREATE TABLE store_sales AS " +
                    "SELECT ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, " +
                    "   ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, cast(ss_wholesale_cost as double) as ss_wholesale_cost,  " +
                    "   cast(ss_list_price as double) as ss_list_price, cast(ss_sales_price as double) as ss_sales_price, " +
                    "   cast(ss_ext_discount_amt as double) as ss_ext_discount_amt, cast(ss_ext_sales_price as double) as ss_ext_sales_price, " +
                    "   cast(ss_ext_wholesale_cost as double) as ss_ext_wholesale_cost, cast(ss_ext_list_price as double) as ss_ext_list_price, " +
                    "   cast(ss_ext_tax as double) as ss_ext_tax, cast(ss_coupon_amt as double) as ss_coupon_amt, " +
                    "   cast(ss_net_paid as double) as ss_net_paid, cast(ss_net_paid_inc_tax as double) as ss_net_paid_inc_tax," +
                    "   cast(ss_net_profit as double) as ss_net_profit " +
                    "FROM tpcds.tiny.store_sales");
        }
    }

    private static void createTpcdsTimeDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "time_dim")) {
            queryRunner.execute(session, "CREATE TABLE time_dim AS " +
                    "SELECT t_time_sk, cast(t_time_id as varchar) as t_time_id, t_time, t_hour, t_minute, t_second,  " +
                    "   cast(t_am_pm as varchar) as t_am_pm, cast(t_shift as varchar) as t_shift, " +
                    "   cast(t_sub_shift as varchar) as t_sub_shift, cast(t_meal_time as varchar) as t_meal_time " +
                    "FROM tpcds.tiny.time_dim");
        }
    }

    private static void createTpcdsWarehouse(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "warehouse")) {
            queryRunner.execute(session, "CREATE TABLE warehouse AS " +
                    "SELECT w_warehouse_sk, cast(w_warehouse_id as varchar) as w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, " +
                    "   cast(w_street_number as varchar) as w_street_number, w_street_name, cast(w_street_type as varchar) as w_street_type, " +
                    "   cast(w_suite_number as varchar) as w_suite_number, w_city, w_county, cast(w_state as varchar) as w_state," +
                    "   cast(w_zip as varchar) as w_zip, w_country, cast(w_gmt_offset as double) as w_gmt_offset " +
                    "FROM tpcds.tiny.warehouse");
        }
    }

    private static void createTpcdsWebPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_page")) {
            queryRunner.execute(session, "CREATE TABLE web_page AS " +
                    "SELECT wp_web_page_sk, cast(wp_web_page_id as varchar) as wp_web_page_id, cast(wp_rec_start_date as varchar) as wp_rec_start_date, " +
                    "   cast(wp_rec_end_date as varchar) as wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, " +
                    "   cast(wp_autogen_flag as varchar) as wp_autogen_flag, wp_customer_sk, wp_url, cast(wp_type as varchar) as wp_type, " +
                    "   wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count " +
                    "FROM tpcds.tiny.web_page");
        }
    }

    private static void createTpcdsWebReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_returns")) {
            queryRunner.execute(session, "CREATE TABLE web_returns AS " +
                    "SELECT wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, " +
                    "   wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, " +
                    "   wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, cast(wr_return_amt as double) as wr_return_amt, " +
                    "   cast(wr_return_tax as double) as wr_return_tax, cast(wr_return_amt_inc_tax as double) as wr_return_amt_inc_tax, " +
                    "   cast(wr_fee as double) as wr_fee, cast(wr_return_ship_cost as double) as wr_return_ship_cost, " +
                    "   cast(wr_refunded_cash as double) as wr_refunded_cash, cast(wr_reversed_charge as double) as wr_reversed_charge, " +
                    "   cast(wr_account_credit as double) as wr_account_credit, cast(wr_net_loss as double) as wr_net_loss " +
                    "FROM tpcds.tiny.web_returns");
        }
    }

    private static void createTpcdsWebSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_sales")) {
            queryRunner.execute(session, "CREATE TABLE web_sales AS " +
                    "SELECT ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, " +
                    "   ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, " +
                    "   ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, " +
                    "   ws_promo_sk, ws_order_number, ws_quantity, cast(ws_wholesale_cost as double) as ws_wholesale_cost, " +
                    "   cast(ws_list_price as double) as ws_list_price, cast(ws_sales_price as double) as ws_sales_price, " +
                    "   cast(ws_ext_discount_amt as double) as ws_ext_discount_amt, cast(ws_ext_sales_price as double) as ws_ext_sales_price, " +
                    "   cast(ws_ext_wholesale_cost as double) as ws_ext_wholesale_cost, cast(ws_ext_list_price as double) as ws_ext_list_price, " +
                    "   cast(ws_ext_tax as double) as ws_ext_tax, cast(ws_coupon_amt as double) as ws_coupon_amt, " +
                    "   cast(ws_ext_ship_cost as double) as ws_ext_ship_cost, cast(ws_net_paid as double) as ws_net_paid, " +
                    "   cast(ws_net_paid_inc_tax as double) as ws_net_paid_inc_tax, cast(ws_net_paid_inc_ship as double) as ws_net_paid_inc_ship, " +
                    "   cast(ws_net_paid_inc_ship_tax as double) as ws_net_paid_inc_ship_tax, cast(ws_net_profit as double) as ws_net_profit " +
                    "FROM tpcds.tiny.web_sales");
        }
    }

    private static void createTpcdsWebSite(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_site")) {
            queryRunner.execute(session, "CREATE TABLE web_site AS " +
                    "SELECT web_site_sk, cast(web_site_id as varchar) as web_site_id, cast(web_rec_start_date as varchar) as web_rec_start_date, " +
                    "   cast(web_rec_end_date as varchar) as web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, " +
                    "   web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, cast(web_company_name as varchar) as web_company_name, " +
                    "   cast(web_street_number as varchar) as web_street_number, web_street_name, cast(web_street_type as varchar) as web_street_type, " +
                    "   cast(web_suite_number as varchar) as web_suite_number, web_city, web_county, cast(web_state as varchar) as web_state, " +
                    "   cast(web_zip as varchar) as web_zip, web_country, cast(web_gmt_offset as double) as web_gmt_offset, " +
                    "   cast(web_tax_percentage as double) as web_tax_percentage " +
                    "FROM tpcds.tiny.web_site");
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
