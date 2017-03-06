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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingPageSinkProvider;
import com.facebook.presto.testing.TestingSplitManager;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBeginQuery
        extends AbstractTestQueryFramework
{
    private TestMetadata metadata;

    protected TestBeginQuery()
            throws Exception
    {
        super(TestBeginQuery::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .build();
        return new DistributedQueryRunner(session, 1);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        metadata = new TestMetadata();
        getQueryRunner().installPlugin(new TestPlugin(metadata));
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @BeforeMethod
    public void beforeMethod()
            throws Exception
    {
        metadata.clear();
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
    }

    @Test
    public void testCreateTableAsSelectSameConnector()
            throws Exception
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("CREATE TABLE nation_copy AS SELECT * FROM nation");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation VALUES (12345, 'name', 54321, 'comment')");
    }

    @Test
    public void testInsertSelectSameConnector()
            throws Exception
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM nation");
    }

    @Test
    public void testSelect()
            throws Exception
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("SELECT * FROM nation");
    }

    private void assertBeginQuery(String query)
    {
        metadata.resetCounters();
        computeActual(query);
        assertEquals(metadata.begin.get(), 1);
        assertEquals(metadata.end.get(), 1);
        metadata.resetCounters();
    }

    private void assertNoBeginQuery(String query)
    {
        metadata.resetCounters();
        computeActual(query);
        assertEquals(metadata.begin.get(), 0);
        assertEquals(metadata.end.get(), 0);
        metadata.resetCounters();
    }

    private static class TestPlugin
            implements Plugin
    {
        private final TestMetadata metadata;

        private TestPlugin(TestMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public ConnectorHandleResolver getHandleResolver()
                {
                    return new TestingHandleResolver();
                }

                @Override
                public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(metadata);
                }
            });
        }
    }

    private static class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return (transactionHandle, session, split, columns) -> new FixedPageSource(ImmutableList.of());
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }

    private static class TestMetadata
            extends TestingMetadata
    {
        private final AtomicInteger begin = new AtomicInteger();
        private final AtomicInteger end = new AtomicInteger();

        @Override
        public void beginQuery(ConnectorSession session)
        {
            begin.incrementAndGet();
        }

        @Override
        public void cleanupQuery(ConnectorSession session)
        {
            end.incrementAndGet();
        }

        public void resetCounters()
        {
            begin.set(0);
            end.set(0);
        }
    }
}
