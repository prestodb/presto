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
package com.facebook.presto;

import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedTuple;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    private Handle handle;
    private Session session;

    @BeforeClass(alwaysRun = true)
    public void setupDatabase()
            throws Exception
    {
        Logging.initialize();

        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());
        TpchMetadata tpchMetadata = new TpchMetadata("");

        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate CHAR(10) NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority BIGINT NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        handle.execute("CREATE INDEX custkey_index ON orders (custkey)");
        TpchTableHandle ordersHandle = tpchMetadata.getTableHandle(new SchemaTableName(TINY_SCHEMA_NAME, ORDERS.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(ordersHandle), handle, createTpchRecordSet(ORDERS, ordersHandle.getScaleFactor()));

        handle.execute("CREATE TABLE lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber BIGINT,\n" +
                "  quantity BIGINT NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate CHAR(10) NOT NULL,\n" +
                "  commitdate CHAR(10) NOT NULL,\n" +
                "  receiptdate CHAR(10) NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        TpchTableHandle lineItemHandle = tpchMetadata.getTableHandle(new SchemaTableName(TINY_SCHEMA_NAME, LINE_ITEM.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(lineItemHandle), handle, createTpchRecordSet(LINE_ITEM, lineItemHandle.getScaleFactor()));

        session = setUpQueryFramework();
    }

    @AfterClass(alwaysRun = true)
    public void cleanupDatabase()
            throws Exception
    {
        tearDownQueryFramework();
        handle.close();
    }

    protected Session getSession()
    {
        return session;
    }

    protected abstract int getNodeCount();

    protected abstract Session setUpQueryFramework()
            throws Exception;

    protected void tearDownQueryFramework()
            throws Exception
    {
    }

    protected abstract MaterializedResult computeActual(@Language("SQL") String sql);

    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, false);
    }

    protected void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, true);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQuery(actual, expected, false);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQuery(actual, expected, true);
    }

    private static final Logger log = Logger.get(AbstractTestQueryFramework.class);

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActual(actual);
        Duration actualTime = Duration.nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTupleInfos());
        log.info("FINISHED in presto: %s, h2: %s, total: %s", actualTime, Duration.nanosSince(expectedStart), Duration.nanosSince(start));

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            fail(format("not equal\nActual %s rows:\n    %s\nExpected %s rows:\n    %s\n",
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(actualSet, 100)),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(expectedSet, 100))));
        }
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<TupleInfo> resultTupleInfos)
    {
        return new MaterializedResult(
                handle.createQuery(sql)
                        .map(tupleMapper(resultTupleInfos))
                        .list(),
                resultTupleInfos
        );
    }

    private static ResultSetMapper<MaterializedTuple> tupleMapper(final List<TupleInfo> tupleInfos)
    {
        return new ResultSetMapper<MaterializedTuple>()
        {
            @Override
            public MaterializedTuple map(int index, ResultSet resultSet, StatementContext ctx)
                    throws SQLException
            {
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(tupleInfos.size() == count, "tuple info does not match result");
                List<Object> row = new ArrayList<>(count);
                for (int i = 1; i <= count; i++) {
                    TupleInfo.Type type = tupleInfos.get(i - 1).getType();
                    switch (type) {
                        case BOOLEAN:
                            boolean booleanValue = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                row.add(null);
                            }
                            else {
                                row.add(booleanValue);
                            }
                            break;
                        case FIXED_INT_64:
                            long longValue = resultSet.getLong(i);
                            if (resultSet.wasNull()) {
                                row.add(null);
                            }
                            else {
                                row.add(longValue);
                            }
                            break;
                        case DOUBLE:
                            double doubleValue = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                row.add(null);
                            }
                            else {
                                row.add(doubleValue);
                            }
                            break;
                        case VARIABLE_BINARY:
                            String value = resultSet.getString(i);
                            if (resultSet.wasNull()) {
                                row.add(null);
                            }
                            else {
                                row.add(value);
                            }
                            break;
                        default:
                            throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedTuple(MaterializedResult.DEFAULT_PRECISION, row);
            }
        };
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        String vars = Joiner.on(',').join(nCopies(tableMetadata.getColumns().size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableMetadata.getTable().getTableName(), vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    batch.execute();
                    return;
                }
                PreparedBatchPart part = batch.add();
                for (int column = 0; column < tableMetadata.getColumns().size(); column++) {
                    ColumnMetadata columnMetadata = tableMetadata.getColumns().get(column);
                    switch (columnMetadata.getType()) {
                        case BOOLEAN:
                            part.bind(column, cursor.getBoolean(column));
                            break;
                        case LONG:
                            part.bind(column, cursor.getLong(column));
                            break;
                        case DOUBLE:
                            part.bind(column, cursor.getDouble(column));
                            break;
                        case STRING:
                            part.bind(column, new String(cursor.getString(column), UTF_8));
                            break;
                    }
                }
            }
            batch.execute();
        }
    }

    protected Function<MaterializedTuple, String> onlyColumnGetter()
    {
        return new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        };
    }

    protected String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getPlan(SqlParser.createStatement(query), planType);
    }

    protected String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getGraphvizPlan(SqlParser.createStatement(query), planType);
    }

    protected QueryExplainer getQueryExplainer()
    {
        MetadataManager metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true));
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
        SplitManager splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of(new DualSplitManager(new InMemoryNodeManager())));
        IndexManager indexManager = new IndexManager();
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true);
        List<PlanOptimizer> optimizers = new PlanOptimizersFactory(metadata, splitManager, indexManager, featuresConfig).get();
        return new QueryExplainer(session, optimizers, metadata, featuresConfig.isExperimentalSyntaxEnabled());
    }
}
