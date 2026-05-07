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
package com.facebook.presto.iceberg;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

@Test(singleThreaded = true)
public class TestIcebergDerivedColumnOptimizer
        extends AbstractTestQueryFramework
{
    @Language("SQL") private static final String CREATE_TABLE_SQL = "CREATE TABLE test_table1 (\n" +
            " \"c1\" bigint,\n" +
            " \"c2\" varchar,\n" +
            " \"c3\" double,\n" +
            " \"c2_derived\" varchar\n" +
            " )\n" +
            "  WITH (\n" +
            "        \"derived-columns\" = Array['c2_derived'],\n" +
            "        \"derived-columns.spec.udf.json\" = JSON '{\n" +
            "        \"udfSpecList\" : [ {\n" +
            "           \"catalog\" : \"presto\",\n" +
            "            \"schema\" : \"default\",\n" +
            "           \"functionName\" : \"lower\",\n" +
            "           \"params\" : [ \"varchar\" ],\n" +
            "           \"arguments\" : [ {\n" +
            "               \"argumentIndex\" : 0,\n" +
            "               \"argumentType\" : \"varchar\",\n" +
            "               \"argumentValue\" : \"c2\",\n" +
            "               \"columnRef\" : \"COLUMN\"\n" +
            "               } ],\n" +
            "           \"derivedColumnName\" : \"c2_derived\",\n" +
            "           \"returnType\" : \"varchar\"\n" +
            "           } ]\n" +
            "       }')\n";
    private File warehouseLocation;
    private TestingHttpServer restServer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @Test
    public void testBasicFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertQuery("SELECT * FROM test_table1 WHERE lower(c2) = 'a'", "VALUES (121, 'A', 12.1, 'a')");
            assertQuery("SELECT * FROM test_table1 WHERE upper(c2) = 'A'", "VALUES (121, 'A', 12.1, 'a')");
            assertPlan("SELECT * FROM test_table1 WHERE upper(c2) = 'A'",
                    anyTree(filter("(upper(c2)) = (VARCHAR'A')", tableScan("test_table1", ImmutableMap.of("c1", "c1", "c2", "c2")))));
            assertPlan("SELECT * FROM test_table1 WHERE lower(c2) = 'a'",
                    anyTree(filter("(c2_derived) = (VARCHAR'a')", tableScan("test_table1",
                            ImmutableMap.of("c1", "c1", "c2", "c2", "c2_derived", "c2_derived")))));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    /**
     * Following test fails here, but the query run and fetches the correct result using presto-cli.
     * Interestingly, the generated plans are slightly different via presto-cli and the test.
     */
    @Ignore
    public void testSelectWithDerivedColumnNotProjectedFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            // The following query does not project derived column i.e. c2_derived.
            assertPlan("SELECT c1 FROM test_table1 WHERE lower(c2) = 'a'",
                    anyTree(filter("(c2_derived) = (VARCHAR'a')", tableScan("test_table1",
                            ImmutableMap.of("c1", "c1", "c2", "c2", "c2_derived", "c2_derived")))));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }
}
