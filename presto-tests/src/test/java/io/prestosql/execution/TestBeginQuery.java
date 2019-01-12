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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingHandleResolver;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingPageSinkProvider;
import io.prestosql.testing.TestingSplitManager;
import io.prestosql.testing.TestingTransactionHandle;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBeginQuery
        extends AbstractTestQueryFramework
{
    private TestMetadata metadata;

    protected TestBeginQuery()
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
    {
        metadata = new TestMetadata();
        getQueryRunner().installPlugin(new TestPlugin(metadata));
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod()
    {
        metadata.clear();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        metadata.clear();
        metadata = null;
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
    }

    @Test
    public void testCreateTableAsSelectSameConnector()
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("CREATE TABLE nation_copy AS SELECT * FROM nation");
    }

    @Test
    public void testInsert()
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation VALUES (12345, 'name', 54321, 'comment')");
    }

    @Test
    public void testInsertSelectSameConnector()
    {
        assertNoBeginQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        assertBeginQuery("INSERT INTO nation SELECT * FROM nation");
    }

    @Test
    public void testSelect()
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
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
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
