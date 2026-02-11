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
package com.facebook.presto.flightshim;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimQueries
        extends AbstractTestFlightShimBase
{
    private ArrowBlockBuilder blockBuilder;

    protected abstract String getConnectorId();

    protected abstract String getConnectionUrl();

    @Override
    protected void setup(Injector injector)
    {
        TypeManager typeManager = injector.getInstance(TypeManager.class);
        blockBuilder = new ArrowBlockBuilder(typeManager);
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        return new FlightShimQueryRunner();
    }

    @Test
    public void testSelectColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, LINENUMBER_COLUMN));
    }

    @Test
    public void testDateColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, SHIPDATE_COLUMN));
    }

    @Test
    public void testFloatingPointColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, QUANTITY_COLUMN, EXTENDEDPRICE_COLUMN));
    }

    protected void assertSelectQueryFromColumns(List<String> tpchColumnNames)
    {
        @Language("SQL") String query = format("SELECT %s FROM %s", String.join(",", tpchColumnNames), TPCH_TABLE);
        assertQuery(query);
    }

    protected List<JdbcColumnHandle> getHandlesFromSelectQuery(String sql)
    {
        String sqlLower = sql.toLowerCase(Locale.ENGLISH);
        int start = sqlLower.indexOf("select");
        if (start < 0) {
            throw new RuntimeException("Expected 'SELECT' in query: " + sql);
        }
        start += "select".length();
        int stop = sqlLower.indexOf("from");
        if (stop < start) {
            throw new RuntimeException("Expected 'FROM' in query: " + sql);
        }
        String columnsString = sql.substring(start, stop);
        List<String> columns = Arrays.stream(columnsString.split(",")).map(String::trim).collect(Collectors.toList());

        if (columns.isEmpty()) {
            throw new RuntimeException("No columns found in query: " + sql);
        }

        ImmutableList.Builder<JdbcColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        for (String column : columns) {
            switch (column) {
                case ORDERKEY_COLUMN:
                    columnHandlesBuilder.add(getOrderKeyColumn());
                    break;
                case LINENUMBER_COLUMN:
                    columnHandlesBuilder.add(getLineNumberColumn());
                    break;
                case QUANTITY_COLUMN:
                    columnHandlesBuilder.add(getQuantityColumn());
                    break;
                case EXTENDEDPRICE_COLUMN:
                    columnHandlesBuilder.add(getExtendedPriceColumn());
                    break;
                case LINESTATUS_COLUMN:
                    columnHandlesBuilder.add(getLineStatusColumn());
                    break;
                case SHIPDATE_COLUMN:
                    columnHandlesBuilder.add(getShipDateColumn());
                    break;
                default:
                    throw new RuntimeException("Unknown column handle for: " + column);
            }
        }

        return columnHandlesBuilder.build();
    }

    protected class FlightShimQueryRunner
            implements QueryRunner
    {
        @Override
        public Session getDefaultSession()
        {
            return ((QueryRunner) getExpectedQueryRunner()).getDefaultSession();
        }

        @Override
        public MaterializedResult execute(String sql)
        {
            try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                    FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
                List<JdbcColumnHandle> columnHandles = getHandlesFromSelectQuery(sql);
                Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(columnHandles)));

                List<Page> pages = new ArrayList<>();
                try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                    while (stream.next()) {
                        List<Block> blocks = new ArrayList<>();
                        for (JdbcColumnHandle columnHandle : columnHandles) {
                            FieldVector vector = stream.getRoot().getVector(columnHandle.getColumnName());
                            Block block = blockBuilder.buildBlockFromFieldVector(vector, columnHandle.getColumnType(), null);
                            blocks.add(block);
                        }
                        pages.add(new Page(stream.getRoot().getRowCount(), blocks.toArray(new Block[0])));
                    }
                }

                List<Type> types = columnHandles.stream().map(JdbcColumnHandle::getColumnType).collect(Collectors.toList());
                MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), types);
                resultBuilder.pages(pages);
                return resultBuilder.build();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getNodeCount()
        {
            return 0;
        }

        @Override
        public TransactionManager getTransactionManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Metadata getMetadata()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitManager getSplitManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageSourceManager getPageSourceManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public NodePartitioningManager getNodePartitioningManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPlanOptimizerManager getPlanOptimizerManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCheckerProviderManager getPlanCheckerProviderManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public StatsCalculator getStatsCalculator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<EventListener> getEventListeners()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TestingAccessControlManager getAccessControl()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionOptimizerManager getExpressionManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MaterializedResult execute(Session session, String sql)
        {
            return execute(sql);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(Session session, String table)
        {
            return false;
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
        }

        @Override
        public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
        }

        @Override
        public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
        {
        }

        @Override
        public Lock getExclusiveLock()
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    }
}
