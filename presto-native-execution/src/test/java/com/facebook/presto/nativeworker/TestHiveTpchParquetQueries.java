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

import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;
import java.util.Optional;

import static org.testng.Assert.assertNotNull;

public abstract class TestHiveTpchParquetQueries
        extends TestHiveTpchQueries
{
    protected TestHiveTpchParquetQueries(boolean useThrift)
    {
        super(useThrift);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String dataDirectory = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");

        assertNotNull(prestoServerPath);
        assertNotNull(dataDirectory);

        return HiveExternalWorkerQueryRunner.createNativeQueryRunner(
                dataDirectory,
                prestoServerPath,
                Optional.ofNullable(workerCount).map(Integer::parseInt),
                0,
                useThrift,
                ImmutableMap.of(
                        "optimizer.optimize-hash-generation", "false",
                        "parse-decimal-literals-as-double", "true",
                        "inline-sql-functions", "false",
                        "use-alternative-function-signatures", "true",
                        "experimental.table-writer-merge-operator-enabled", "false"),
                ImmutableMap.of(
                        "hive.storage-format", "PARQUET",
                        "hive.pushdown-filter-enabled", "true",
                        "hive.parquet.pushdown-filter-enabled", "true"));
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        String security = "sql-standard";

        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableMap.of(
                        "parse-decimal-literals-as-double", "true",
                        "regex-library", "RE2J",
                        "offset-clause-enabled", "true"),
                security,
                ImmutableMap.of(
                        "hive.storage-format", "PARQUET",
                        "hive.pushdown-filter-enabled", "true"),
                Optional.of(Paths.get(dataDirectory)));

        createLineitemParquet(queryRunner);
        createOrdersParquet(queryRunner);
        createNationParquet(queryRunner);
        createCustomerParquet(queryRunner);
        createPartParquet(queryRunner);
        createPartSuppParquet(queryRunner);
        createRegionParquet(queryRunner);
        createSupplierParquet(queryRunner);

        return queryRunner;
    }

    private static void createLineitemParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem")) {
            queryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
        }
    }

    private static void createOrdersParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders")) {
            queryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders");
        }
    }

    private static void createNationParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation")) {
            queryRunner.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        }
    }

    private static void createCustomerParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "customer")) {
            queryRunner.execute("CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer");
        }
    }

    private static void createPartParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "part")) {
            queryRunner.execute("CREATE TABLE part AS SELECT * FROM tpch.tiny.part");
        }
    }

    private static void createPartSuppParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "partsupp")) {
            queryRunner.execute("CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp");
        }
    }

    private static void createRegionParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "region")) {
            queryRunner.execute("CREATE TABLE region AS SELECT * FROM tpch.tiny.region");
        }
    }

    private static void createSupplierParquet(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "supplier")) {
            queryRunner.execute("CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier");
        }
    }
}
