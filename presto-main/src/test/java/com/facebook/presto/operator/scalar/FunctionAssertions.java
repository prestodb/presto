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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry.FunctionListBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.StaticOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.AnalysisContext;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.InterpretedFilterFunction;
import com.facebook.presto.sql.planner.InterpretedProjectionFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.Threads;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.scalar.FunctionAssertions.TestSplit.createNormalSplit;
import static com.facebook.presto.operator.scalar.FunctionAssertions.TestSplit.createRecordSetSplit;
import static com.facebook.presto.spi.ColumnType.BOOLEAN;
import static com.facebook.presto.spi.ColumnType.DOUBLE;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class FunctionAssertions
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test-%s"));

    private static final Page SOURCE_PAGE = new Page(
            createLongsBlock(1234L),
            createStringsBlock("hello"),
            createDoublesBlock(12.34),
            createBooleansBlock(true),
            createLongsBlock(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
            createStringsBlock("%el%"),
            createStringsBlock((String) null));

    private static final Page ZERO_CHANNEL_PAGE = new Page(1);

    private static final Map<Input, Type> INPUT_TYPES = ImmutableMap.<Input, Type>builder()
            .put(new Input(0), Type.BIGINT)
            .put(new Input(1), Type.VARCHAR)
            .put(new Input(2), Type.DOUBLE)
            .put(new Input(3), Type.BOOLEAN)
            .put(new Input(4), Type.BIGINT)
            .put(new Input(5), Type.VARCHAR)
            .put(new Input(6), Type.VARCHAR)
            .build();

    private static final Map<Symbol, Input> INPUT_MAPPING = ImmutableMap.<Symbol, Input>builder()
            .put(new Symbol("bound_long"), new Input(0))
            .put(new Symbol("bound_string"), new Input(1))
            .put(new Symbol("bound_double"), new Input(2))
            .put(new Symbol("bound_boolean"), new Input(3))
            .put(new Symbol("bound_timestamp"), new Input(4))
            .put(new Symbol("bound_pattern"), new Input(5))
            .put(new Symbol("bound_null_string"), new Input(6))
            .build();

    private static final TupleDescriptor TUPLE_DESCRIPTOR = new TupleDescriptor(ImmutableList.copyOf(transform(INPUT_MAPPING.entrySet(), new Function<Entry<Symbol, Input>, Field>()
    {
        @Override
        public Field apply(Entry<Symbol, Input> entry)
        {
            return Field.newUnqualified(entry.getKey().getName(), INPUT_TYPES.get(entry.getValue()));
        }
    })));

    private static final DataStreamProvider DATA_STREAM_PROVIDER = new TestDataStreamProvider();
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("scan");

    private final MetadataManager metadataManager = new MetadataManager();
    private final ExpressionCompiler compiler = new ExpressionCompiler(metadataManager);

    public FunctionAssertions() {}

    public FunctionAssertions addFunctions(List<FunctionInfo> functionInfos)
    {
        metadataManager.addFunctions(functionInfos);
        return this;
    }

    public FunctionAssertions addScalarFunctions(Class<?> clazz)
    {
        metadataManager.addFunctions(new FunctionListBuilder().scalar(clazz).build());
        return this;
    }

    public void assertFunction(String projection, Object expected)
    {
        if (expected instanceof Integer) {
            expected = ((Integer) expected).longValue();
        }
        else if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(Charsets.UTF_8);
        }
        assertEquals(selectSingleValue(projection), expected);
    }

    public void assertFunctionNull(String projection)
    {
        assertNull(selectSingleValue(projection));
    }

    public Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, SESSION);
    }

    public Object selectSingleValue(String projection, Session session)
    {
        List<Object> results = executeProjectionWithAll(projection, session);
        HashSet<Object> resultSet = new HashSet<>(results);

        // we should only have a single result
        assertTrue(resultSet.size() == 1, "Expected only one result unique result, but got " + resultSet);

        return Iterables.getOnlyElement(resultSet);
    }

    public List<Object> executeProjectionWithAll(String projection, Session session)
    {
        checkNotNull(projection, "projection is null");

        Expression projectionExpression = createExpression(projection);

        List<Object> results = new ArrayList<>();

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(TRUE_LITERAL, projectionExpression);
        Type expressionType = Type.fromRaw(operatorFactory.getTupleInfos().get(0).getType());
        Object directOperatorValue = selectSingleValue(operatorFactory, session);
        results.add(directOperatorValue);

        // interpret
        Object interpretedValue = selectSingleValue(interpretedFilterProject(TRUE_LITERAL, projectionExpression, expressionType, session));
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(TRUE_LITERAL, projectionExpression);
        Object scanOperatorValue = selectSingleValue(scanProjectOperatorFactory, createNormalSplit(), session);
        results.add(scanOperatorValue);

        // execute over record set
        Object recordValue = selectSingleValue(scanProjectOperatorFactory, createRecordSetSplit(), session);
        results.add(recordValue);

        //
        // If the projection does not need bound values, execute query using full engine
        if (!needsBoundValue(projectionExpression)) {
            try {
                LocalQueryRunner runner = new LocalQueryRunner(session, EXECUTOR);
                MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");
                assertEquals(result.getTupleInfos().size(), 1);
                assertEquals(result.getMaterializedTuples().size(), 1);
                Object queryResult = Iterables.getOnlyElement(result.getMaterializedTuples()).getField(0);
                results.add(queryResult);
            }
            catch (RuntimeException e) {
                // todo remove this when analyzer supports null types and full numeric type promotion
            }
        }

        return results;
    }

    public static Object selectSingleValue(OperatorFactory operatorFactory, Session session)
    {
        Operator operator = operatorFactory.createOperator(createDriverContext(session));
        return selectSingleValue(operator);
    }

    public static Object selectSingleValue(SourceOperatorFactory operatorFactory, Split split, Session session)
    {
        SourceOperator operator = operatorFactory.createOperator(createDriverContext(session));
        operator.addSplit(split);
        operator.noMoreSplits();
        return selectSingleValue(operator);
    }

    public static Object selectSingleValue(Operator operator)
    {
        Page output = getAtMostOnePage(operator, SOURCE_PAGE);

        assertNotNull(output);
        assertEquals(output.getPositionCount(), 1);
        assertEquals(output.getChannelCount(), 1);

        Block block = output.getBlock(0);
        assertEquals(block.getPositionCount(), 1);

        BlockCursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        if (cursor.isNull()) {
            return null;
        }
        else {
            return cursor.getTuple().getObjectValue();
        }
    }

    public void assertFilter(String filter, boolean expected, boolean withNoInputColumns)
    {
        List<Boolean> results = executeFilterWithAll(filter, SESSION, withNoInputColumns);
        HashSet<Boolean> resultSet = new HashSet<>(results);

        // we should only have a single result
        assertTrue(resultSet.size() == 1, "Expected only [" + expected + "] result unique result, but got " + resultSet);

        assertEquals((boolean) Iterables.getOnlyElement(resultSet), expected);
    }

    public List<Boolean> executeFilterWithAll(String filter, Session session, boolean executeWithNoInputColumns)
    {
        checkNotNull(filter, "filter is null");

        Expression filterExpression = createExpression(filter);

        List<Boolean> results = new ArrayList<>();

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(filterExpression, TRUE_LITERAL);
        Type expressionType = Type.fromRaw(operatorFactory.getTupleInfos().get(0).getType());
        results.add(executeFilter(operatorFactory, session));

        if (executeWithNoInputColumns) {
            // execute as standalone operator
            operatorFactory = compileFilterWithNoInputColumns(filterExpression);
            results.add(executeFilterWithNoInputColumns(operatorFactory, session));
        }

        // interpret
        boolean interpretedValue = executeFilter(interpretedFilterProject(filterExpression, TRUE_LITERAL, expressionType, session));
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(filterExpression, TRUE_LITERAL);
        boolean scanOperatorValue = executeFilter(scanProjectOperatorFactory, createNormalSplit(), session);
        results.add(scanOperatorValue);

        // execute over record set
        boolean recordValue = executeFilter(scanProjectOperatorFactory, createRecordSetSplit(), session);
        results.add(recordValue);

        //
        // If the filter does not need bound values, execute query using full engine
        if (!needsBoundValue(filterExpression)) {
            try {
                LocalQueryRunner runner = new LocalQueryRunner(session, EXECUTOR);
                MaterializedResult result = runner.execute("SELECT TRUE FROM dual WHERE " + filter);
                assertEquals(result.getTupleInfos().size(), 1);

                Boolean queryResult;
                if (result.getMaterializedTuples().isEmpty()) {
                    queryResult = false;
                }
                else {
                    assertEquals(result.getMaterializedTuples().size(), 1);
                    queryResult = (Boolean) Iterables.getOnlyElement(result.getMaterializedTuples()).getField(0);
                }
                results.add(queryResult);
            }
            catch (SemanticException e) {
                // todo remove this when analyzer supports null types and full numeric type promotion
            }
        }

        return results;
    }

    private static boolean executeFilterWithNoInputColumns(OperatorFactory operatorFactory, Session session)
    {
        return executeFilterWithNoInputColumns(operatorFactory.createOperator(createDriverContext(session)));
    }

    private static boolean executeFilter(OperatorFactory operatorFactory, Session session)
    {
        return executeFilter(operatorFactory.createOperator(createDriverContext(session)));
    }

    private static boolean executeFilter(SourceOperatorFactory operatorFactory, Split split, Session session)
    {
        SourceOperator operator = operatorFactory.createOperator(createDriverContext(session));
        operator.addSplit(split);
        operator.noMoreSplits();
        return executeFilter(operator);
    }

    private static boolean executeFilter(Operator operator)
    {
        Page page = getAtMostOnePage(operator, SOURCE_PAGE);

        boolean value;
        if (page != null) {
            assertEquals(page.getPositionCount(), 1);
            assertEquals(page.getChannelCount(), 1);

            BlockCursor cursor = page.getBlock(0).cursor();
            assertTrue(cursor.advanceNextPosition());
            assertTrue(cursor.getBoolean());
            value = true;
        }
        else {
            value = false;
        }
        return value;
    }

    private static boolean executeFilterWithNoInputColumns(Operator operator)
    {
        Page page = getAtMostOnePage(operator, ZERO_CHANNEL_PAGE);

        boolean value;
        if (page != null) {
            assertEquals(page.getPositionCount(), 1);
            assertEquals(page.getChannelCount(), 0);
            value = true;
        }
        else {
            value = false;
        }
        return value;
    }

    private static boolean needsBoundValue(Expression projectionExpression)
    {
        final AtomicBoolean hasQualifiedNameReference = new AtomicBoolean();
        projectionExpression.accept(new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
            {
                hasQualifiedNameReference.set(true);
                return null;
            }
        }, null);
        return hasQualifiedNameReference.get();
    }

    private Operator interpretedFilterProject(Expression filter, Expression projection, Type expressionType, Session session)
    {
        FilterFunction filterFunction = new InterpretedFilterFunction(
                filter,
                INPUT_MAPPING,
                metadataManager,
                session
        );

        ProjectionFunction projectionFunction = new InterpretedProjectionFunction(
                expressionType,
                projection,
                INPUT_MAPPING,
                metadataManager,
                session
        );

        OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(0, filterFunction, ImmutableList.of(projectionFunction));
        return operatorFactory.createOperator(createDriverContext(session));
    }

    private OperatorFactory compileFilterWithNoInputColumns(Expression filter)
    {
        filter = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(ImmutableMap.<Symbol, Input>of()), filter);

        try {
            return compiler.compileFilterAndProjectOperator(0, filter, ImmutableList.<Expression>of(), ImmutableMap.<Input, Type>of(), ImmutableList.<Type>of());
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + filter + ": " + e.getMessage(), e);
        }
    }

    private OperatorFactory compileFilterProject(Expression filter, Expression projection)
    {
        Type projectionType = getExpressionType(projection);

        filter = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), filter);
        projection = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), projection);

        try {
            return compiler.compileFilterAndProjectOperator(0, filter, ImmutableList.of(projection), INPUT_TYPES, ImmutableList.of(projectionType));
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + projection + ": " + e.getMessage(), e);
        }
    }

    private SourceOperatorFactory compileScanFilterProject(Expression filter, Expression projection)
    {
        Type projectionType = getExpressionType(projection);

        filter = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), filter);
        projection = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), projection);

        try {
            return compiler.compileScanFilterAndProjectOperator(
                    0,
                    SOURCE_ID,
                    DATA_STREAM_PROVIDER,
                    ImmutableList.<ColumnHandle>of(),
                    filter,
                    ImmutableList.of(projection),
                    INPUT_TYPES,
                    ImmutableList.of(projectionType));
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + projection + ": " + e.getMessage(), e);
        }
    }

    private static Page getAtMostOnePage(Operator operator, Page sourcePage)
    {
        // add our input page if needed
        if (operator.needsInput()) {
            operator.addInput(sourcePage);
        }

        // try to get the output page
        Page result = operator.getOutput();

        // tell operator to finish
        operator.finish();

        // try to get output until the operator is finished
        while (!operator.isFinished()) {
            // operator should never block
            assertTrue(operator.isBlocked().isDone());

            Page output = operator.getOutput();
            if (output != null) {
                assertNull(result);
                result = output;
            }
        }

        return result;
    }

    private static DriverContext createDriverContext(Session session)
    {
        return new TaskContext(new TaskId("query", "stage", "task"), EXECUTOR, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    private Type getExpressionType(Expression projection)
    {
        return new ExpressionAnalyzer(new Analysis(), SESSION, metadataManager, false).analyze(projection, TUPLE_DESCRIPTOR, new AnalysisContext());
    }

    private static class TestDataStreamProvider
            implements DataStreamProvider
    {
        @Override
        public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
        {
            assertInstanceOf(split, FunctionAssertions.TestSplit.class);
            FunctionAssertions.TestSplit testSplit = (FunctionAssertions.TestSplit) split;
            if (testSplit.isRecordSet()) {
                RecordSet records = InMemoryRecordSet.builder(ImmutableList.of(LONG, STRING, DOUBLE, BOOLEAN, LONG, STRING, STRING)).addRow(
                        1234L,
                        "hello",
                        12.34,
                        true,
                        MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
                        "%el%",
                        null
                ).build();
                return new RecordProjectOperator(operatorContext, records);
            }
            else {
                return new StaticOperator(operatorContext, ImmutableList.of(SOURCE_PAGE));
            }
        }
    }

    static class TestSplit
            implements Split
    {
        static Split createRecordSetSplit()
        {
            return new TestSplit(true);
        }

        static Split createNormalSplit()
        {
            return new TestSplit(false);
        }

        private final boolean recordSet;

        private TestSplit(boolean recordSet)
        {
            this.recordSet = recordSet;
        }

        private boolean isRecordSet()
        {
            return recordSet;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }
}
