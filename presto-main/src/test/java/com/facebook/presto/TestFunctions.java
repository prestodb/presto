package com.facebook.presto;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.AbstractMetadata;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.DualTable;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.InternalSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.antlr.runtime.RecognitionException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFunctions
{
    private static final JsonCodec<TaskInfo> TASK_INFO_CODEC = JsonCodec.jsonCodec(TaskInfo.class);

    private final Metadata metadata = new DualTableMetadata();
    private final DualTableDataStreamProvider dataProvider = new DualTableDataStreamProvider();

    @Test
    public void testConcat()
            throws Exception
    {
        assertFunction("CONCAT('hello', ' world')", "hello world");
        assertFunction("CONCAT('', '')", "");
        assertFunction("CONCAT('what', '')", "what");
        assertFunction("CONCAT('', 'what')", "what");
        assertFunction("CONCAT(CONCAT('this', ' is'), ' cool')", "this is cool");
        assertFunction("CONCAT('this', CONCAT(' is', ' cool'))", "this is cool");
    }

    @Test
    public void testLength()
            throws Exception
    {
        assertFunction("LENGTH('')", 0);
        assertFunction("LENGTH('hello')", 5);
        assertFunction("LENGTH('Quadratically')", 13);
    }

    @Test
    public void testReverse()
            throws Exception
    {
        assertFunction("REVERSE('')", "");
        assertFunction("REVERSE('hello')", "olleh");
        assertFunction("REVERSE('Quadratically')", "yllacitardauQ");
        assertFunction("REVERSE('racecar')", "racecar");
    }

    @Test
    public void testSubstring()
            throws Exception
    {
        assertFunction("SUBSTR('Quadratically', 5, 6)", "ratica");
        assertFunction("SUBSTR('Quadratically', 5, 10)", "ratically");
        assertFunction("SUBSTR('Quadratically', 5, 50)", "ratically");
        assertFunction("SUBSTR('Quadratically', 50, 10)", "");
        assertFunction("SUBSTR('Quadratically', -5, 4)", "call");
        assertFunction("SUBSTR('Quadratically', -5, 40)", "cally");
        assertFunction("SUBSTR('Quadratically', -50, 4)", "");
        assertFunction("SUBSTR('Quadratically', 0, 4)", "");
        assertFunction("SUBSTR('Quadratically', 5, 0)", "");
    }

    @Test
    public void testLeftTrim()
            throws Exception
    {
        assertFunction("LTRIM('')", "");
        assertFunction("LTRIM('   ')", "");
        assertFunction("LTRIM('  hello  ')", "hello  ");
        assertFunction("LTRIM('  hello')", "hello");
        assertFunction("LTRIM('hello  ')", "hello  ");
        assertFunction("LTRIM(' hello world ')", "hello world ");
    }

    @Test
    public void testRightTrim()
            throws Exception
    {
        assertFunction("RTRIM('')", "");
        assertFunction("RTRIM('   ')", "");
        assertFunction("RTRIM('  hello  ')", "  hello");
        assertFunction("RTRIM('  hello')", "  hello");
        assertFunction("RTRIM('hello  ')", "hello");
        assertFunction("RTRIM(' hello world ')", " hello world");
    }

    @Test
    public void testTrim()
            throws Exception
    {
        assertFunction("TRIM('')", "");
        assertFunction("TRIM('   ')", "");
        assertFunction("TRIM('  hello  ')", "hello");
        assertFunction("TRIM('  hello')", "hello");
        assertFunction("TRIM('hello  ')", "hello");
        assertFunction("TRIM(' hello world ')", "hello world");
    }

    @Test
    public void testLower()
            throws Exception
    {
        assertFunction("LOWER('')", "");
        assertFunction("LOWER('Hello World')", "hello world");
        assertFunction("LOWER('WHAT!!')", "what!!");
    }

    @Test
    public void testUpper()
            throws Exception
    {
        assertFunction("UPPER('')", "");
        assertFunction("UPPER('Hello World')", "HELLO WORLD");
        assertFunction("UPPER('what!!')", "WHAT!!");
    }

    private void assertFunction(String projection, long expected)
            throws Exception
    {
        doAssertFunction(projection, expected);
    }

    private void assertFunction(String projection, double expected)
            throws Exception
    {
        doAssertFunction(projection, expected);
    }

    private void assertFunction(String projection, String expected)
            throws Exception
    {
        doAssertFunction(projection, Slices.copiedBuffer(expected, Charsets.UTF_8));
    }

    private void doAssertFunction(String projection, Object expected)
            throws Exception
    {
        checkNotNull(projection, "projection is null");

        Operator operator = plan("SELECT " + projection + " FROM dual");

        List<Tuple> results = getTuples(operator);
        assertEquals(results.size(), 1);
        Tuple tuple = results.get(0);
        assertEquals(tuple.getTupleInfo().getFieldCount(), 1);

        TupleInfo.Type type = tuple.getTupleInfo().getTypes().get(0);
        switch (type) {
            case FIXED_INT_64:
                assertEquals(tuple.getLong(0), expected);
                break;
            case DOUBLE:
                assertEquals(tuple.getDouble(0), expected);
                break;
            case VARIABLE_BINARY:
                assertEquals(tuple.getSlice(0), expected);
                break;
            default:
                throw new AssertionError("unimplemented type: " + type);
        }
    }

    private static List<Tuple> getTuples(Operator root)
    {
        ImmutableList.Builder<Tuple> output = ImmutableList.builder();
        PageIterator iterator = root.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            Preconditions.checkState(page.getChannelCount() == 1, "Expected result to produce 1 channel");

            BlockCursor cursor = Iterables.getOnlyElement(Arrays.asList(page.getBlocks())).cursor();
            while (cursor.advanceNextPosition()) {
                output.add(cursor.getTuple());
            }
        }

        return output.build();
    }

    private Operator plan(String sql)
            throws RecognitionException
    {
        Statement statement = SqlParser.createStatement(sql);

        Analyzer analyzer = new Analyzer(new Session(null, Session.DEFAULT_CATALOG, Session.DEFAULT_SCHEMA), metadata);

        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNode plan = new LogicalPlanner().plan((Query) statement, analysis);
        new PlanPrinter().print(plan, analysis.getTypes());

        SubPlan subplan = new DistributedLogicalPlanner(metadata).createSubplans(plan, analysis.getSymbolAllocator(), true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        ImmutableMap.Builder<TableHandle, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            InternalTableHandle handle = (InternalTableHandle) tableScan.getTable();
            builder.put(handle, new TableScanPlanFragmentSource(new InternalSplit(handle)));
        }

        DataSize maxOperatorMemoryUsage = new DataSize(50, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                new HackPlanFragmentSourceProvider(dataProvider, null, TASK_INFO_CODEC),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage
        );

        return executionPlanner.plan(plan);
    }

    private static class DualTableMetadata
            extends AbstractMetadata
    {
        private final FunctionRegistry functions = new FunctionRegistry();

        @Override
        public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
        {
            return functions.get(name, parameterTypes);
        }

        @Override
        public FunctionInfo getFunction(FunctionHandle handle)
        {
            return functions.get(handle);
        }

        @Override
        public TableMetadata getTable(String catalogName, String schemaName, String tableName)
        {
            checkArgument(tableName.equals(DualTable.NAME), "wrong table name: %s", tableName);
            return DualTable.getMetadata(catalogName, schemaName, tableName);
        }
    }

    private static class DualTableDataStreamProvider
            implements DataStreamProvider
    {
        @Override
        public Operator createDataStream(Split split, List<ColumnHandle> columns)
        {
            checkArgument(columns.size() == 1, "expected exactly one column");
            InternalTable table = DualTable.getInternalTable(DEFAULT_CATALOG, DEFAULT_SCHEMA, DualTable.NAME);
            return new AlignmentOperator(ImmutableList.of(table.getColumn(0)));
        }
    }
}
