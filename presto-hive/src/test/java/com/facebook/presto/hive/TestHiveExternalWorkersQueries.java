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

import com.facebook.presto.Session;
import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;

@Experimental
public class TestHiveExternalWorkersQueries
        extends AbstractTestQueryFramework
{
    protected TestHiveExternalWorkersQueries()
    {
        super(TestHiveExternalWorkersQueries::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getenv("PRESTO_SERVER");
        String baseDataDir = System.getenv("DATA_DIR");

        return createQueryRunner(Optional.ofNullable(prestoServerPath), Optional.ofNullable(baseDataDir).map(Paths::get));
    }

    private static QueryRunner createQueryRunner(Optional<String> prestoServerPath, Optional<Path> baseDataDir)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(baseDataDir.isPresent(), "Path to data files must be specified when testing external workers");
        }

        DistributedQueryRunner defaultQueryRunner = HiveQueryRunner.createQueryRunner(
                ImmutableList.of(NATION),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of("hive.storage-format", "DWRF"),
                baseDataDir);

        // DWRF doesn't support date type. Convert date columns to varchar for lineitem and orders.
        createLineitem(defaultQueryRunner);
        createOrders(defaultQueryRunner);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        // Make query runner with external workers for tests
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of(),
                ImmutableMap.of("optimizer.optimize-hash-generation", "false",
                        "parse-decimal-literals-as-double", "true"),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of(),
                Optional.of(1),
                baseDataDir,
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(TestHiveExternalWorkersQueries.class.getSimpleName());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("config.properties"),
                                format("discovery.uri=%s\n" +
                                        "presto.version=testversion\n" +
                                        "http-server.http.port=%d", discoveryUri, port).getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s\n" +
                                        "node.ip=127.0.0.1\n" +
                                        "node.environment=testing", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        Files.write(catalogDirectoryPath.resolve("hive.properties"), "connector.name=hive".getBytes());

                        return new ProcessBuilder(prestoServerPath.get(), "--logtostderr=1", "--v=1")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                                .redirectError(ProcessBuilder.Redirect.INHERIT)
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));

        return queryRunner;
    }

    private static void createLineitem(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem")) {
            queryRunner.execute("CREATE TABLE lineitem AS " +
                    "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate, " +
                    "   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment " +
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

    @Test
    public void testFiltersAndProjections()
    {
        assertQuery("SELECT * FROM nation");
        assertQuery("SELECT * FROM nation WHERE nationkey = 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery("SELECT * FROM nation WHERE nationkey < 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey > 4");
        assertQuery("SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT substr(comment, 1, 10), length(comment) FROM orders");
    }

    @Test
    public void testFilterPushdown()
    {
        Session filterPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, "pushdown_filter_enabled", "true")
                .build();

        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey = 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey < 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey > 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey IN (1, 3, 5)");

        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.02");
        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount BETWEEN 0.01 AND 0.02");

        // no row passes the filter
        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.2");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) FROM nation");
        assertQuery("SELECT regionkey, count(*) FROM nation GROUP BY regionkey");

        assertQuery("SELECT avg(discount), avg(quantity) FROM lineitem");
        assertQuery("SELECT linenumber, avg(discount), avg(quantity) FROM lineitem GROUP BY linenumber");

        assertQuery("SELECT sum(totalprice) FROM orders");
        assertQuery("SELECT orderpriority, sum(totalprice) FROM orders GROUP BY orderpriority");

        assertQuery("SELECT custkey, min(totalprice), max(orderkey) FROM orders GROUP BY custkey");
    }

    @Test
    public void testTopN()
    {
        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 5");

        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 50");

        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax " +
                "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 10");

        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax " +
                "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 2000");
    }

    @Test
    public void testCast()
    {
        // TODO Fix cast double-to-varchar and boolean-to-varchar.
        //  cast(0.0 as varchar) should return "0.0", not "0".
        //  cast(bool as varchar) should return "TRUE" or "FALSE", not "true" or "false".
        assertQuery("SELECT CAST(linenumber as TINYINT), CAST(linenumber AS SMALLINT), " +
                "CAST(linenumber AS INTEGER), CAST(linenumber AS BIGINT), CAST(quantity AS REAL), " +
                "CAST(orderkey AS DOUBLE), CAST(orderkey AS VARCHAR) FROM lineitem");
    }

    @Test
    public void testValues()
    {
        assertQuery("SELECT 1, 0.24, ceil(4.5), 'A not too short ASCII string'");
    }
}
