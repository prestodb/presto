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
import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.facebook.presto.transaction.TransactionBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * This is integration / unit test suite.
 * The reason for having it here is to ensure that we won't leak memory in MetadataManager
 * while registering catalog -> query Id mapping.
 * This mapping has to be manually cleaned when query finishes execution (Metadata#cleanupQuery method).
 */
@Test(singleThreaded = true)
public class TestMetadataManager
{
    private DistributedQueryRunner queryRunner;
    private MetadataManager metadataManager;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                        .withListSchemaNames(session -> ImmutableList.of("UPPER_CASE_SCHEMA"))
                        .withListTables((session, schemaNameOrNull) -> {
                            throw new UnsupportedOperationException();
                        })
                        .withGetViews((session, prefix) -> ImmutableMap.of())
                        .withGetColumnHandles((session, tableHandle) -> {
                            throw new UnsupportedOperationException();
                        })
                        .withGetTableStatistics(() -> {
                            throw new UnsupportedOperationException();
                        })
                        .build();
                return ImmutableList.of(connectorFactory);
            }
        });
        queryRunner.createCatalog("upper_case_schema_catalog", "mock");
        metadataManager = (MetadataManager) queryRunner.getMetadata();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        metadataManager = null;
    }

    @Test
    public void testMetadataIsClearedAfterQueryFinished()
    {
        @Language("SQL") String sql = "SELECT * FROM nation";
        queryRunner.execute(sql);

        assertEquals(metadataManager.getCatalogsByQueryId().size(), 0);
    }

    @Test
    public void testMetadataIsClearedAfterQueryFailed()
    {
        @Language("SQL") String sql = "SELECT nationkey/0 FROM nation"; // will raise division by zero exception
        try {
            queryRunner.execute(sql);
            fail("expected exception");
        }
        catch (Throwable t) {
            // query should fail
        }

        assertEquals(metadataManager.getCatalogsByQueryId().size(), 0);
    }

    @Test
    public void testMetadataIsClearedAfterQueryCanceled()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        QueryId queryId = dispatchManager.createQueryId();
        dispatchManager.createQuery(
                queryId,
                "slug",
                0,
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        // wait until query starts running
        while (true) {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);
            if (queryInfo.getState().isDone()) {
                assertEquals(queryInfo.getState(), FAILED);
                throw dispatchManager.getDispatchInfo(queryId).get().getFailureInfo().get().toException();
            }
            if (queryInfo.getState() == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        dispatchManager.cancelQuery(queryId);
        assertEquals(metadataManager.getCatalogsByQueryId().size(), 0);
    }

    @Test
    public void testUpperCaseSchemaIsChangedToLowerCase()
    {
        TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(
                        TEST_SESSION,
                        transactionSession -> {
                            List<String> expectedSchemas = ImmutableList.of("information_schema", "upper_case_schema");
                            assertEquals(queryRunner.getMetadata().listSchemaNames(transactionSession, "upper_case_schema_catalog"), expectedSchemas);
                            return null;
                        });
    }

    @Test
    public void testGetTableStatisticsDoesNotThrow()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("ignore_stats_calculator_failures", "true")
                .build();
        TableHandle tableHandle = new TableHandle(new ConnectorId("upper_case_schema_catalog"), new ConnectorTableHandle() {}, TestingTransactionHandle.create(), Optional.empty());
        TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(
                        session,
                        transactionSession -> {
                            queryRunner.getMetadata().getCatalogHandle(transactionSession, "upper_case_schema_catalog");
                            assertEquals(queryRunner.getMetadata().getTableStatistics(transactionSession, tableHandle, ImmutableList.of(), alwaysTrue()), TableStatistics.empty());
                        });
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetTableStatisticsThrows()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .build();
        TableHandle tableHandle = new TableHandle(new ConnectorId("upper_case_schema_catalog"), new ConnectorTableHandle() {}, TestingTransactionHandle.create(), Optional.empty());
        TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(
                        session,
                        transactionSession -> {
                            queryRunner.getMetadata().getCatalogHandle(transactionSession, "upper_case_schema_catalog");
                            assertEquals(queryRunner.getMetadata().getTableStatistics(transactionSession, tableHandle, ImmutableList.of(), alwaysTrue()), TableStatistics.empty());
                        });
    }
}
