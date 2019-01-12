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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.TestingSessionContext;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import io.prestosql.transaction.TransactionBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.RUNNING;
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
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        QueryId queryId = queryManager.createQueryId();
        queryManager.createQuery(
                queryId,
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        // wait until query starts running
        while (true) {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            if (queryInfo.getState().isDone()) {
                assertEquals(queryInfo.getState(), FAILED);
                throw queryInfo.getFailureInfo().toException();
            }
            if (queryInfo.getState() == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        queryManager.cancelQuery(queryId);
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
}
