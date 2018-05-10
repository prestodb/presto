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

import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.facebook.presto.transaction.TransactionBuilder;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
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
        queryRunner.installPlugin(new Plugin() {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(new MockConnectorFactory(
                        session -> ImmutableList.of("UPPER_CASE_SCHEMA"),
                        (session, schemaNameOrNull) -> {
                            throw new UnsupportedOperationException();
                        },
                        (session, tableHandle) -> {
                            throw new UnsupportedOperationException();
                        }));
            }
        });
        queryRunner.createCatalog("upper_case_schema_catalog", "mock");
        metadataManager = (MetadataManager) queryRunner.getMetadata();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
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
        QueryId queryId = queryManager.createQuery(
                queryManager.createQueryId(),
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem").getQueryId();

        // wait until query starts running
        while (true) {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
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
