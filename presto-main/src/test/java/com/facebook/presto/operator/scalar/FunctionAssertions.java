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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
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
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.ValuesOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.ExpressionAnalysis;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.InterpretedFilterFunction;
import com.facebook.presto.sql.planner.InterpretedProjectionFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.scalar.FunctionAssertions.TestSplit.createNormalSplit;
import static com.facebook.presto.operator.scalar.FunctionAssertions.TestSplit.createRecordSetSplit;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeExpressionsWithSymbols;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions.canonicalizeExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class FunctionAssertions
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final Page SOURCE_PAGE = new Page(
            createLongsBlock(1234L),
            createStringsBlock("hello"),
            createDoublesBlock(12.34),
            createBooleansBlock(true),
            createLongsBlock(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
            createStringsBlock("%el%"),
            createStringsBlock((String) null));

    private static final Page ZERO_CHANNEL_PAGE = new Page(1);

    private static final Map<Input, Type> INPUT_TYPES = ImmutableMap.<Input, Type>builder()
            .put(new Input(0), BIGINT)
            .put(new Input(1), VARCHAR)
            .put(new Input(2), DOUBLE)
            .put(new Input(3), BOOLEAN)
            .put(new Input(4), BIGINT)
            .put(new Input(5), VARCHAR)
            .put(new Input(6), VARCHAR)
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

    private static final Map<Symbol, Type> SYMBOL_TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("bound_long"), BIGINT)
            .put(new Symbol("bound_string"), VARCHAR)
            .put(new Symbol("bound_double"), DOUBLE)
            .put(new Symbol("bound_boolean"), BOOLEAN)
            .put(new Symbol("bound_timestamp"), BIGINT)
            .put(new Symbol("bound_pattern"), VARCHAR)
            .put(new Symbol("bound_null_string"), VARCHAR)
            .build();

    private static final DataStreamProvider DATA_STREAM_PROVIDER = new TestDataStreamProvider();
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("scan");

    private final ConnectorSession session;
    private final LocalQueryRunner runner;
    private final MetadataManager metadataManager;
    private final ExpressionCompiler compiler;

    public FunctionAssertions()
    {
        this(SESSION);
    }

    public FunctionAssertions(ConnectorSession session)
    {
        this.session = checkNotNull(session, "session is null");
        runner = new LocalQueryRunner(session, EXECUTOR);
        metadataManager = runner.getMetadata();
        compiler = new ExpressionCompiler(metadataManager);
    }

    public FunctionAssertions addFunctions(List<FunctionInfo> functionInfos)
    {
        metadataManager.addFunctions(functionInfos);
        return this;
    }

    public FunctionAssertions addScalarFunctions(Class<?> clazz)
    {
        metadataManager.addFunctions(new FunctionListBuilder().scalar(clazz).getFunctions());
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
        Object actual = selectSingleValue(projection);
        assertEquals(actual, expected);
    }

    public void assertFunctionNull(String projection)
    {
        assertNull(selectSingleValue(projection));
    }

    public Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, session);
    }

    public Object selectSingleValue(String projection, ConnectorSession session)
    {
        List<Object> results = executeProjectionWithAll(projection, session);
        HashSet<Object> resultSet = new HashSet<>(results);

        // we should only have a single result
        assertTrue(resultSet.size() == 1, "Expected only one result unique result, but got " + resultSet);

        return Iterables.getOnlyElement(resultSet);
    }

    public List<Object> executeProjectionWithAll(String projection, ConnectorSession session)
    {
        checkNotNull(projection, "projection is null");

        Expression projectionExpression = createExpression(projection, metadataManager, SYMBOL_TYPES);

        List<Object> results = new ArrayList<>();

        //
        // If the projection does not need bound values, execute query using full engine
        if (!needsBoundValue(projectionExpression)) {
            MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");
            assertEquals(result.getTypes().size(), 1);
            assertEquals(result.getMaterializedRows().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedRows()).getField(0);
            results.add(queryResult);
        }

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(TRUE_LITERAL, projectionExpression);
        Object directOperatorValue = selectSingleValue(operatorFactory, session);
        results.add(directOperatorValue);

        // interpret
        Object interpretedValue = selectSingleValue(interpretedFilterProject(TRUE_LITERAL, projectionExpression, session));
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
            MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");
            assertEquals(result.getTypes().size(), 1);
            assertEquals(result.getMaterializedRows().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedRows()).getField(0);
            results.add(queryResult);
        }

        return results;
    }

    public Object selectSingleValue(OperatorFactory operatorFactory, ConnectorSession session)
    {
        Operator operator = operatorFactory.createOperator(createDriverContext(session));
        return selectSingleValue(operator);
    }

    public Object selectSingleValue(SourceOperatorFactory operatorFactory, Split split, ConnectorSession session)
    {
        SourceOperator operator = operatorFactory.createOperator(createDriverContext(session));
        operator.addSplit(split);
        operator.noMoreSplits();
        return selectSingleValue(operator);
    }

    public Object selectSingleValue(Operator operator)
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
            return cursor.getObjectValue(session);
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

    public List<Boolean> executeFilterWithAll(String filter, ConnectorSession session, boolean executeWithNoInputColumns)
    {
        checkNotNull(filter, "filter is null");

        Expression filterExpression = createExpression(filter, metadataManager, SYMBOL_TYPES);

        List<Boolean> results = new ArrayList<>();

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(filterExpression, TRUE_LITERAL);
        results.add(executeFilter(operatorFactory, session));

        if (executeWithNoInputColumns) {
            // execute as standalone operator
            operatorFactory = compileFilterWithNoInputColumns(filterExpression);
            results.add(executeFilterWithNoInputColumns(operatorFactory, session));
        }

        // interpret
        boolean interpretedValue = executeFilter(interpretedFilterProject(filterExpression, TRUE_LITERAL, session));
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
            MaterializedResult result = runner.execute("SELECT TRUE FROM dual WHERE " + filter);
            assertEquals(result.getTypes().size(), 1);

            Boolean queryResult;
            if (result.getMaterializedRows().isEmpty()) {
                queryResult = false;
            }
            else {
                assertEquals(result.getMaterializedRows().size(), 1);
                queryResult = (Boolean) Iterables.getOnlyElement(result.getMaterializedRows()).getField(0);
            }
            results.add(queryResult);
        }

        return results;
    }

    public static Expression createExpression(String expression, Metadata metadata, Map<Symbol, Type> symbolTypes)
    {
        Expression parsedExpression = SqlParser.createExpression(expression);

        final ExpressionAnalysis analysis = analyzeExpressionsWithSymbols(SESSION, metadata, symbolTypes, ImmutableList.of(parsedExpression));
        Expression rewrittenExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);

                // cast expression if coercion is registered
                Type coercion = analysis.getCoercion(node);
                if (coercion != null) {
                    rewrittenExpression = new Cast(rewrittenExpression, coercion.getName());
                }

                return rewrittenExpression;
            }
        }, parsedExpression);

        return canonicalizeExpression(rewrittenExpression);
    }

    private static boolean executeFilterWithNoInputColumns(OperatorFactory operatorFactory, ConnectorSession session)
    {
        return executeFilterWithNoInputColumns(operatorFactory.createOperator(createDriverContext(session)));
    }

    private static boolean executeFilter(OperatorFactory operatorFactory, ConnectorSession session)
    {
        return executeFilter(operatorFactory.createOperator(createDriverContext(session)));
    }

    private static boolean executeFilter(SourceOperatorFactory operatorFactory, Split split, ConnectorSession session)
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

    private Operator interpretedFilterProject(Expression filter, Expression projection, ConnectorSession session)
    {
        FilterFunction filterFunction = new InterpretedFilterFunction(
                filter,
                SYMBOL_TYPES,
                INPUT_MAPPING,
                metadataManager,
                session
        );

        ProjectionFunction projectionFunction = new InterpretedProjectionFunction(
                projection,
                SYMBOL_TYPES,
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

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(SESSION, metadataManager, INPUT_TYPES, ImmutableList.of(filter));

        try {
            return compiler.compileFilterAndProjectOperator(0, filter, ImmutableList.<Expression>of(), expressionTypes, session.getTimeZoneKey());
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
        filter = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), filter);
        projection = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), projection);

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(SESSION, metadataManager, INPUT_TYPES, ImmutableList.of(filter, projection));

        try {
            return compiler.compileFilterAndProjectOperator(0, filter, ImmutableList.of(projection), expressionTypes, session.getTimeZoneKey());
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
        filter = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), filter);
        projection = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), projection);

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(SESSION, metadataManager, INPUT_TYPES, ImmutableList.of(filter, projection));

        try {
            return compiler.compileScanFilterAndProjectOperator(
                    0,
                    SOURCE_ID,
                    DATA_STREAM_PROVIDER,
                    ImmutableList.<ColumnHandle>of(),
                    filter,
                    ImmutableList.of(projection),
                    expressionTypes,
                    session.getTimeZoneKey());
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

    private static DriverContext createDriverContext(ConnectorSession session)
    {
        return new TaskContext(new TaskId("query", "stage", "task"), EXECUTOR, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    private static class TestDataStreamProvider
            implements DataStreamProvider
    {
        @Override
        public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
        {
            assertInstanceOf(split.getConnectorSplit(), FunctionAssertions.TestSplit.class);
            FunctionAssertions.TestSplit testSplit = (FunctionAssertions.TestSplit) split.getConnectorSplit();
            if (testSplit.isRecordSet()) {
                RecordSet records = InMemoryRecordSet.builder(ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BOOLEAN, BIGINT, VARCHAR, VARCHAR)).addRow(
                        1234L,
                        "hello",
                        12.34,
                        true,
                        new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis(),
                        "%el%",
                        null
                ).build();
                return new RecordProjectOperator(operatorContext, records);
            }
            else {
                return new ValuesOperator(operatorContext, ImmutableList.of(SOURCE_PAGE));
            }
        }
    }

    static class TestSplit
            implements ConnectorSplit
    {
        static Split createRecordSetSplit()
        {
            return new Split("test", new TestSplit(true));
        }

        static Split createNormalSplit()
        {
            return new Split("test", new TestSplit(false));
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
