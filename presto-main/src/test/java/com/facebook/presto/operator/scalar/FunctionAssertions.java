/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.AbstractMetadata;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.DualTable;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.HackPlanFragmentSourceProvider;
import com.facebook.presto.slice.Slice;
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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class FunctionAssertions
{
    private FunctionAssertions()
    {
    }

    private static final Metadata METADATA = new DualTableMetadata();
    private static final DualTableDataStreamProvider DATA_PROVIDER = new DualTableDataStreamProvider();

    public static void assertFunction(String projection, long expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunction(String projection, double expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunction(String projection, String expected)
    {
        Slice value = (Slice) selectSingleValue(projection);
        if (value == null) {
            assertEquals(value, expected);
        } else {
            assertEquals(value.toString(UTF_8), expected);
        }
    }

    public static void assertFunction(String projection, boolean expected)
    {
        assertEquals(selectBooleanValue(projection), expected);
    }

    public static Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, new Session(null, DEFAULT_CATALOG, DEFAULT_SCHEMA));
    }

    public static Object selectSingleValue(String projection, Session session)
    {
        checkNotNull(projection, "projection is null");

        Operator operator = plan("SELECT " + projection + " FROM dual", session);

        List<Tuple> results = getTuples(operator);
        assertEquals(results.size(), 1);
        Tuple tuple = results.get(0);
        assertEquals(tuple.getTupleInfo().getFieldCount(), 1);

        if (tuple.isNull(0)) {
            return null;
        }

        Type type = tuple.getTupleInfo().getTypes().get(0);
        switch (type) {
            case FIXED_INT_64:
                return tuple.getLong(0);
            case DOUBLE:
                return tuple.getDouble(0);
            case VARIABLE_BINARY:
                return tuple.getSlice(0);
            default:
                throw new AssertionError("unimplemented type: " + type);
        }
    }

    public static Object selectBooleanValue(String projection)
    {
        checkNotNull(projection, "projection is null");

        Operator operator = plan("SELECT 1 FROM dual where " + projection, new Session(null, DEFAULT_CATALOG, DEFAULT_SCHEMA));

        List<Tuple> results = getTuples(operator);
        return !results.isEmpty();
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

    private static Operator plan(String sql, Session session)
    {
        Statement statement = SqlParser.createStatement(sql);

        Analyzer analyzer = new Analyzer(session, METADATA);

        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanNode plan = new LogicalPlanner(session, METADATA, idAllocator).plan(analysis);

        SubPlan subplan = new DistributedLogicalPlanner(METADATA, idAllocator).createSubplans(plan, analysis.getSymbolAllocator(), true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        ImmutableMap.Builder<PlanNodeId, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            InternalTableHandle handle = (InternalTableHandle) tableScan.getTable();
            builder.put(tableScan.getId(), new TableScanPlanFragmentSource(new InternalSplit(handle)));
        }

        DataSize maxOperatorMemoryUsage = new DataSize(50, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                session,
                METADATA,
                new HackPlanFragmentSourceProvider(DATA_PROVIDER, null, new QueryManagerConfig()),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<PlanNodeId, ExchangePlanFragmentSource>of(),
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
        public FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes)
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
