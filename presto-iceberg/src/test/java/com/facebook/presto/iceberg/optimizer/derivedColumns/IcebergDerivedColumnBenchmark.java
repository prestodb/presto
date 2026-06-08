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
package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * Result "com.facebook.presto.iceberg.optimizer.derivedColumns.IcebergDerivedColumnBenchmark.benchmarkIcebergTableTpcds08":
 * 1286.743 ±(99.9%) 56.801 ms/op [Average]
 * (min, avg, max) = (1233.635, 1286.743, 1362.567), stdev = 37.570
 * CI (99.9%): [1229.942, 1343.544] (assumes normal distribution)
 */
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class IcebergDerivedColumnBenchmark
{
    public static final String SCHEMA_NAME = "ice_bench";
    @Language("SQL") public static final String TPCDS_Q15_MODIFIED_SQL = "--TPC-DS Q15 \n" +
            "select ca_zip\n" +
            "from catalog_sales\n" +
            "   , customer\n" +
            "   , customer_address\n" +
            "   , date_dim\n" +
            "where cs_bill_customer_sk = c_customer_sk\n" +
            "  and c_current_addr_sk = ca_address_sk\n" +
            "  and (substr(ca_zip, 1, 5) in ('85669', '86197', '88274', '83405', '86475',\n" +
            "                                '85392', '85460', '80348', '81792')\n" +
            "    or upper(ca_state) in ('CA', 'WA', 'GA')\n" +
            "    or cs_sales_price > 500) LIMIT 1000\n";
    public static final String CUSTOMER_ADDRESS = "customer_address";
    public static final String STORE = "store";
    protected DistributedQueryRunner queryRunner;
    protected DistributedQueryRunner queryRunnerWithDerivedCol;
    private final List<String> tables = ImmutableList.of("customer", CUSTOMER_ADDRESS, "catalog_sales", "date_dim", STORE);
    private File warehouseLocation;
    private TestingHttpServer restServer;

    public IcebergDerivedColumnBenchmark()
    {
        queryRunner = getQueryRunner(false);
        queryRunnerWithDerivedCol = getQueryRunner(true);
    }

    public DistributedQueryRunner getQueryRunner(boolean derivedColumn)
    {
        try {
            if (restServer == null) {
                warehouseLocation = Files.newTemporaryFolder();
                restServer = getRestServer(warehouseLocation.getAbsolutePath());
                restServer.start();
            }
            ImmutableMap<String, String> extraConnectorProperties;
            if (derivedColumn) {
                extraConnectorProperties = ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .put("iceberg.derived_columns.enable", "true")
                        .build();
            }
            else {
                extraConnectorProperties = ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .build();
            }
            return IcebergQueryRunner.builder()
                    .setCatalogType(REST)
                    .setNodeCount(OptionalInt.of(4))
                    .setExtraConnectorProperties(extraConnectorProperties)
                    .setCreateTpchTables(false)
                    .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                    .setAddJmxPlugin(false)
                    .setSchemaName(SCHEMA_NAME)
                    .setTpcdsProperties(ImmutableMap.of("tpcds.use-varchar-type", "true"))
                    .build().getQueryRunner();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Setup
    public void setUp()
    {
        queryRunner.execute(format("CREATE SCHEMA IF NOT EXISTS %s", SCHEMA_NAME));
        for (String table : tables) {
            queryRunner.execute(format("CREATE TABLE %s.%s AS SELECT * from tpcds.sf1.%s", SCHEMA_NAME, table, table));
        }
        queryRunner.execute(format("ALTER TABLE %s.%s ADD COLUMN ca_zip_derived VARCHAR(5) AS substr(ca_zip, 1, 5) PERSISTENT", SCHEMA_NAME, CUSTOMER_ADDRESS));
        queryRunner.execute(format("ALTER TABLE %s.%s ADD COLUMN ca_state_derived VARCHAR(2) AS upper(ca_state) PERSISTENT", SCHEMA_NAME, CUSTOMER_ADDRESS));
        queryRunner.execute(format("UPDATE %s.%s SET ca_zip_derived = substr(ca_zip, 1, 5)", SCHEMA_NAME, CUSTOMER_ADDRESS));
        queryRunner.execute(format("UPDATE %s.%s SET ca_state_derived = upper(ca_state)", SCHEMA_NAME, CUSTOMER_ADDRESS));
    }

    @TearDown
    public void tearDown()
            throws Exception
    {
        for (String table : tables) {
            queryRunner.execute(format("DROP TABLE IF EXISTS %s.%s", SCHEMA_NAME, table));
        }
        queryRunner.execute(format("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME));
        queryRunner.close();
        queryRunnerWithDerivedCol.close();
        queryRunner = null;
        queryRunnerWithDerivedCol = null;
        if (restServer != null) {
            restServer.stop();
            restServer = null;
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Benchmark
    public MaterializedResult benchmarkIcebergTableTpcds08()
    {
        return queryRunner.execute(TPCDS_Q15_MODIFIED_SQL);
    }

    @Benchmark
    public MaterializedResult benchmarkIcebergTableTpcds08WithDerivedCol()
    {
        return queryRunnerWithDerivedCol.execute(TPCDS_Q15_MODIFIED_SQL);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .forks(1)
                .measurementIterations(1)
                .mode(AverageTime)
                .include(".*" + IcebergDerivedColumnBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
