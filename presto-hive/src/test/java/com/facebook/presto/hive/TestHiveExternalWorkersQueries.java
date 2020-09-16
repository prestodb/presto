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

        Path tempDirectoryPath = Files.createTempDirectory(TestHiveExternalWorkersQueries.class.getSimpleName());

        // Make query runner with external workers for tests
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of(),
                ImmutableMap.of("optimizer.optimize-hash-generation", "false"),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of(),
                Optional.of(1),
                baseDataDir,
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        if (workerIndex == 0) {
                            // Write discovery URL to /tmp/config.properties
                            Files.write(tempDirectoryPath.resolve("config.properties"),
                                    format("discovery.uri=%s\n", discoveryUri).getBytes());
                        }
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
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT substr(comment, 1, 10), length(comment) FROM orders");
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
}
