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
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.facebook.presto.SystemSessionProperties.QUERY_ANALYZER_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.REPORT_INTERRUPTED_PLANNING_AS_TIMEOUT;
import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_PLANNING_TIMEOUT;
import static com.facebook.presto.tests.QueryAssertions.assertQueryFails;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

@Test(singleThreaded = true)
public class TestQueryAnalyzerTimeout
{
    private static final String PLANNING_SOURCE = "planning";
    private static final ErrorCodeSupplier METASTORE_ERROR = () -> new ErrorCode(0x7fff_0000, "TEST_METASTORE_ERROR", EXTERNAL);

    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("planner.report-interrupted-planning-as-timeout", "true")
                .build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                // The view lookup runs during analysis, and the schema listing of an information_schema query during
                // planning. Each blocks until the analyzer timeout interrupts it, then fails with an error caused by the
                // interrupt, as a remote metastore client does; the view lookup reports the client's own external error
                // code. For table closed_channel the view lookup fails as an interrupted channel read does; for table
                // user_error it drops the interrupt and fails for an unrelated reason; for table early_interruption it
                // fails at once, before the timeout, with an interruption from elsewhere.
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withGetViews((session, prefix) -> {
                            if ("early_interruption".equals(prefix.getTableName())) {
                                throw new PrestoException(GENERIC_INTERNAL_ERROR, "metastore call failed", new InterruptedException());
                            }
                            try {
                                new CountDownLatch(1).await();
                            }
                            catch (InterruptedException e) {
                                if ("closed_channel".equals(prefix.getTableName())) {
                                    throw new UncheckedIOException("metastore call failed", new ClosedByInterruptException());
                                }
                                if ("user_error".equals(prefix.getTableName())) {
                                    throw new PrestoException(NOT_SUPPORTED, "table is not supported");
                                }
                                throw new PrestoException(METASTORE_ERROR, "metastore call failed", new RuntimeException(e));
                            }
                            throw new AssertionError("view lookup was not interrupted");
                        })
                        .withListSchemaNames(session -> {
                            if (!session.getSource().orElse("").equals(PLANNING_SOURCE)) {
                                return ImmutableList.of("default");
                            }
                            try {
                                new CountDownLatch(1).await();
                            }
                            catch (InterruptedException e) {
                                throw new PrestoException(GENERIC_INTERNAL_ERROR, "schema listing failed", new RuntimeException(e));
                            }
                            throw new AssertionError("schema listing was not interrupted");
                        })
                        .build(),
                        new LingeringInterruptConnectorFactory());
            }
        });
        queryRunner.createCatalog("blocking", "mock");
        queryRunner.createCatalog("lingering", "lingering");
        // A warm planner leaves the scheduling test's slow call, not the optimizers, to reach the timeout.
        queryRunner.execute("SELECT * FROM tpch.tiny.region");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test(timeOut = 60_000)
    public void testInterruptedAnalysisIsPlanningTimeout()
    {
        assertPlanningTimeout(session(), "SELECT * FROM blocking.default.t", "TEST_METASTORE_ERROR", "metastore call failed");
    }

    @Test(timeOut = 60_000)
    public void testInterruptedPlanningIsPlanningTimeout()
    {
        Session session = Session.builder(session())
                .setSource(PLANNING_SOURCE)
                .build();
        assertPlanningTimeout(session, "SELECT * FROM blocking.information_schema.tables", "GENERIC_INTERNAL_ERROR", "schema listing failed");
    }

    @Test(timeOut = 60_000)
    public void testInterruptedChannelIsPlanningTimeout()
    {
        assertPlanningTimeout(session(), "SELECT * FROM blocking.default.closed_channel", "GENERIC_INTERNAL_ERROR", "metastore call failed");
    }

    @Test(timeOut = 60_000)
    public void testInterruptedPermissionCheckIsPlanningTimeout()
    {
        assertPlanningTimeout(session(), "SELECT * FROM lingering.tiny.nation", "GENERIC_INTERNAL_ERROR", "permission check failed");
    }

    @Test(timeOut = 60_000)
    public void testInterruptedSchedulingIsPlanningTimeout()
    {
        Session session = Session.builder(session())
                .setSource(PLANNING_SOURCE)
                .build();
        assertPlanningTimeout(session, "SELECT * FROM lingering.tiny.region", "TEST_METASTORE_ERROR", "split enumeration failed");
    }

    @Test(timeOut = 60_000)
    public void testOtherFailureAfterTimeoutKeepsItsError()
    {
        assertQueryFails(queryRunner, session(), "SELECT * FROM blocking.default.user_error", "table is not supported");
    }

    @Test(timeOut = 60_000)
    public void testInterruptionBeforeTimeoutKeepsItsError()
    {
        assertQueryFails(queryRunner, session(), "SELECT * FROM blocking.default.early_interruption", "metastore call failed");
    }

    @Test(timeOut = 60_000)
    public void testInterruptedCallKeepsItsErrorWhenDisabled()
    {
        Session session = Session.builder(session())
                .setSystemProperty(REPORT_INTERRUPTED_PLANNING_AS_TIMEOUT, "false")
                .build();
        assertQueryFails(queryRunner, session, "SELECT * FROM blocking.default.t", "metastore call failed");
    }

    private void assertPlanningTimeout(Session session, String sql, String interruptedCallError, String interruptedCallMessage)
    {
        RuntimeException failure = expectThrows(RuntimeException.class, () -> queryRunner.execute(session, sql));
        assertEquals(failure.getMessage(), format("The query planner exceeded the timeout of 2.00s. The interrupted call failed with %s.", interruptedCallError));
        assertEquals(failure.getCause().getCause().getMessage(), interruptedCallMessage);

        BasicQueryInfo queryInfo = queryRunner.getCoordinator().getDispatchManager().getQueries().stream()
                .filter(query -> query.getQuery().equals(sql))
                .max(comparing(query -> query.getQueryId().getId()))
                .orElseThrow(AssertionError::new);
        assertEquals(queryInfo.getErrorCode(), QUERY_PLANNING_TIMEOUT.toErrorCode());
        assertEquals(queryInfo.getErrorType(), INTERNAL_ERROR);
    }

    private Session session()
    {
        return Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(QUERY_ANALYZER_TIMEOUT, "2s")
                .build();
    }

    private static void awaitInterrupt()
    {
        try {
            new CountDownLatch(1).await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void failIfInterrupted(String message)
    {
        if (Thread.interrupted()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, message, new InterruptedException());
        }
    }

    // A TPCH catalog whose permission check and split enumeration fail on a pending interrupt, as a remote permission
    // service or metastore client does. The table lookup during analysis, or for source planning the input extraction
    // at the end of planning, waits for the analyzer timeout's interrupt and returns with the interrupt still pending,
    // as a call that restores the interrupt instead of failing does.
    private static class LingeringInterruptConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "lingering";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TpchHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            ConnectorSplitManager splitManager = new TpchSplitManager(context.getNodeManager(), 1);
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return TpchTransactionHandle.INSTANCE;
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new TpchMetadata(catalogName)
                    {
                        @Override
                        public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
                        {
                            if (!session.getSource().orElse("").equals(PLANNING_SOURCE)) {
                                awaitInterrupt();
                            }
                            return super.getTableHandle(session, tableName);
                        }

                        @Override
                        public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
                        {
                            awaitInterrupt();
                            return super.getInfo(layoutHandle);
                        }
                    };
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return (transaction, session, layout, splitSchedulingContext) -> {
                        if (Thread.interrupted()) {
                            // Wrapped, as a retrying client gives up, so the relabel must name the cause's error code.
                            throw new RuntimeException("split enumeration failed", new PrestoException(METASTORE_ERROR, "split source failed", new InterruptedException()));
                        }
                        return splitManager.getSplits(transaction, session, layout, splitSchedulingContext);
                    };
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }

                @Override
                public ConnectorAccessControl getAccessControl()
                {
                    return new ConnectorAccessControl()
                    {
                        @Override
                        public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<Subfield> columnOrSubfieldNames)
                        {
                            failIfInterrupted("permission check failed");
                        }
                    };
                }
            };
        }
    }
}
