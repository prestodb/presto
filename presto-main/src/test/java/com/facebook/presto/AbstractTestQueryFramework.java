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
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedRow;
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

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.NullType.NULL;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
        if (handle != null) {
            handle.close();
        }
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

    public void assertQueryOrdered(@Language("SQL") String sql)
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

    private static final Logger log = Logger.get(AbstractTestQueries.class);

    public void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActual(actual);
        Duration actualTime = Duration.nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTypes());
        log.info("FINISHED in presto: %s, h2: %s, total: %s", actualTime, Duration.nanosSince(expectedStart), Duration.nanosSince(start));

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
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

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return new MaterializedResult(
                handle.createQuery(sql)
                        .map(rowMapper(resultTypes))
                        .list(),
                resultTypes
        );
    }

    private static ResultSetMapper<MaterializedRow> rowMapper(final List<? extends Type> types)
    {
        return new ResultSetMapper<MaterializedRow>()
        {
            @Override
            public MaterializedRow map(int index, ResultSet resultSet, StatementContext ctx)
                    throws SQLException
            {
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "type does not match result");
                List<Object> row = new ArrayList<>(count);
                for (int i = 1; i <= count; i++) {
                    Type type = types.get(i - 1);
                    if (BOOLEAN.equals(type)) {
                        boolean booleanValue = resultSet.getBoolean(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(booleanValue);
                        }
                    }
                    else if (BIGINT.equals(type)) {
                        long longValue = resultSet.getLong(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(longValue);
                        }
                    }
                    else if (DOUBLE.equals(type)) {
                        double doubleValue = resultSet.getDouble(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(doubleValue);
                        }
                    }
                    else if (VARCHAR.equals(type)) {
                        String stringValue = resultSet.getString(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(stringValue);
                        }
                    }
                    else if (DATE.equals(type)) {
                        Date dateValue = resultSet.getDate(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(dateValue);
                        }
                    }
                    else if (TIME.equals(type)) {
                        Time timeValue = resultSet.getTime(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timeValue);
                        }
                    }
                    else if (TIMESTAMP.equals(type)) {
                        Timestamp timestampValue = resultSet.getTimestamp(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timestampValue);
                        }
                    }
                    else if (NULL.equals(type)) {
                        Object objectValue = resultSet.getObject(i);
                        checkState(resultSet.wasNull(), "Expected a null value, but got %s", objectValue);
                        row.add(null);
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, row);
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
                    Type type = tableMetadata.getColumns().get(column).getType();
                    if (BOOLEAN.equals(type)) {
                        part.bind(column, cursor.getBoolean(column));
                    }
                    else if (BIGINT.equals(type)) {
                        part.bind(column, cursor.getLong(column));
                    }
                    else if (DOUBLE.equals(type)) {
                        part.bind(column, cursor.getDouble(column));
                    }
                    else if (VARCHAR.equals(type)) {
                        part.bind(column, new String(cursor.getString(column), UTF_8));
                    }
                }
            }
            batch.execute();
        }
    }

    public Function<MaterializedRow, String> onlyColumnGetter()
    {
        return new Function<MaterializedRow, String>()
        {
            @Override
            public String apply(MaterializedRow input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        };
    }

    public String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getPlan(SqlParser.createStatement(query), planType);
    }

    public String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getGraphvizPlan(SqlParser.createStatement(query), planType);
    }

    private QueryExplainer getQueryExplainer()
    {
        MetadataManager metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true), new TypeRegistry());
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
        SplitManager splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of(new DualSplitManager(new InMemoryNodeManager())));
        IndexManager indexManager = new IndexManager();
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true);
        List<PlanOptimizer> optimizers = new PlanOptimizersFactory(metadata, splitManager, indexManager, featuresConfig).get();
        return new QueryExplainer(session, optimizers, metadata, featuresConfig.isExperimentalSyntaxEnabled());
    }
}
