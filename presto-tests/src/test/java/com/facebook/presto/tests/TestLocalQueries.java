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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.plugin.blackhole.BlackHoleConnectorFactory;
import com.facebook.presto.plugin.blackhole.BlackHoleMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.FIELD_LENGTH_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.PAGE_PROCESSING_DELAY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.ROWS_PER_PAGE_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.SPLIT_COUNT_PROPERTY;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private static final String BLACKHOLE_CATALOG = "blackhole";

    public TestLocalQueries()
    {
        super(TestLocalQueries::createLocalQueryRunner);
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        // add blackhole catalog
        localQueryRunner.createCatalog(
                BLACKHOLE_CATALOG,
                new BlackHoleConnectorFactory(),
                ImmutableMap.of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testShowColumnStats()
            throws Exception
    {
        // FIXME Add tests for more complex scenario with more stats
        MaterializedResult result = computeActual("SHOW STATS FOR nation");

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE)
                .row("regionkey", null, 5.0, 0.0, null)
                .row("name", null, 25.0, 0.0, null)
                .row("comment", null, 25.0, 0.0, null)
                .row("nationkey", null, 25.0, 0.0, null)
                .row(null, null, null, null, 25.0)
                .build();

        assertEquals(result, expectedStatistics);
    }

    @Test(timeOut = 10 * 60 * 1000)
    public void testPlannerWithPlentyColumnsTable()
            throws Exception
    {
        Session session = Session.builder(getSession())
                .setCatalog(BLACKHOLE_CATALOG)
                .build();

        String tableName = "plenty_columns";
        createPlentyColumnsTable(session, tableName);

        computeActual(session, String.format("SELECT * FROM %s", tableName));
        computeActual(session, String.format("SELECT * FROM %s a, %s b WHERE a.column_1 = b.column_1", tableName, tableName));
    }

    private void createPlentyColumnsTable(Session session, String tableName)
    {
        // I wish CREATE TABLE was easier in tests
        LocalQueryRunner queryRunner = (LocalQueryRunner) getQueryRunner();
        queryRunner.inTransaction(session, txSession -> {
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                    new SchemaTableName(BlackHoleMetadata.SCHEMA_NAME, tableName),
                    IntStream.range(0, 2000)
                            .mapToObj(i -> new ColumnMetadata("column_" + i, BigintType.BIGINT))
                            .collect(toImmutableList()),
                    ImmutableMap.of(
                            SPLIT_COUNT_PROPERTY, 0,
                            PAGES_PER_SPLIT_PROPERTY, 0,
                            ROWS_PER_PAGE_PROPERTY, 0,
                            FIELD_LENGTH_PROPERTY, 0,
                            PAGE_PROCESSING_DELAY, Duration.succinctNanos(0L)));

            queryRunner.getMetadata().createTable(txSession, BLACKHOLE_CATALOG, tableMetadata, true);
            return null;
        });
    }
}
