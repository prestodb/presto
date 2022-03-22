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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.Utils;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.analyzer.ExpressionAnalysis;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRowBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.createTimestampsWithTimezoneBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.SqlToRowExpressionTranslator.translate;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class FunctionAssertions
        implements Closeable
{
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

    private static final SqlParser SQL_PARSER = new SqlParser();

    // Increase the number of fields to generate a wide column
    private static final int TEST_ROW_NUMBER_OF_FIELDS = 2500;
    private static final RowType TEST_ROW_TYPE = createTestRowType(TEST_ROW_NUMBER_OF_FIELDS);
    private static final Block TEST_ROW_DATA = createTestRowData(TEST_ROW_TYPE);

    private static final Page SOURCE_PAGE = new Page(
            createLongsBlock(1234L),
            createStringsBlock("hello"),
            createDoublesBlock(12.34),
            createBooleansBlock(true),
            createLongsBlock(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
            createStringsBlock("%el%"),
            createStringsBlock((String) null),
            createTimestampsWithTimezoneBlock(packDateTimeWithZone(new DateTime(1970, 1, 1, 0, 1, 0, 999, DateTimeZone.UTC).getMillis(), TimeZoneKey.getTimeZoneKey("Z"))),
            createSlicesBlock(Slices.wrappedBuffer((byte) 0xab)),
            createIntsBlock(1234),
            TEST_ROW_DATA);

    private static final Page ZERO_CHANNEL_PAGE = new Page(1);

    private static final Map<VariableReferenceExpression, Integer> INPUT_MAPPING = ImmutableMap.<VariableReferenceExpression, Integer>builder()
            .put(new VariableReferenceExpression("bound_long", BIGINT), 0)
            .put(new VariableReferenceExpression("bound_string", VARCHAR), 1)
            .put(new VariableReferenceExpression("bound_double", DOUBLE), 2)
            .put(new VariableReferenceExpression("bound_boolean", BOOLEAN), 3)
            .put(new VariableReferenceExpression("bound_timestamp", BIGINT), 4)
            .put(new VariableReferenceExpression("bound_pattern", VARCHAR), 5)
            .put(new VariableReferenceExpression("bound_null_string", VARCHAR), 6)
            .put(new VariableReferenceExpression("bound_timestamp_with_timezone", TIMESTAMP_WITH_TIME_ZONE), 7)
            .put(new VariableReferenceExpression("bound_binary_literal", VARBINARY), 8)
            .put(new VariableReferenceExpression("bound_integer", INTEGER), 9)
            .put(new VariableReferenceExpression("bound_row", TEST_ROW_TYPE), 10)
            .build();

    private static final TypeProvider SYMBOL_TYPES = TypeProvider.fromVariables(INPUT_MAPPING.keySet());

    private static final PageSourceProvider PAGE_SOURCE_PROVIDER = new TestPageSourceProvider();
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("scan");

    private final Session session;
    private final LocalQueryRunner runner;
    private final Metadata metadata;
    private final ExpressionCompiler compiler;

    public FunctionAssertions()
    {
        this(TEST_SESSION);
    }

    public FunctionAssertions(Session session)
    {
        this(session, new FeaturesConfig());
    }

    public FunctionAssertions(Session session, FeaturesConfig featuresConfig)
    {
        this.session = requireNonNull(session, "session is null");
        runner = new LocalQueryRunner(session, featuresConfig);
        metadata = runner.getMetadata();
        compiler = runner.getExpressionCompiler();
    }

    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return runner.getFunctionAndTypeManager();
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public FunctionAssertions addFunctions(List<? extends SqlFunction> functionInfos)
    {
        metadata.registerBuiltInFunctions(functionInfos);
        return this;
    }

    public FunctionAssertions addScalarFunctions(Class<?> clazz)
    {
        metadata.registerBuiltInFunctions(new FunctionListBuilder().scalars(clazz).getFunctions());
        return this;
    }

    public void assertFunction(String projection, Type expectedType, Object expected)
    {
        if (expected instanceof Slice) {
            expected = ((Slice) expected).toStringUtf8();
        }

        Object actual = selectSingleValue(projection, expectedType, compiler);
        assertEquals(actual, expected);
    }

    public void assertFunctionWithError(String projection, Type expectedType, double expected, double delta)
    {
        Number actual = (Number) selectSingleValue(projection, expectedType, compiler);
        assertEquals(actual.doubleValue(), expected, delta);
    }

    public void assertFunctionString(String projection, Type expectedType, String expected)
    {
        Object actual = selectSingleValue(projection, expectedType, compiler);
        assertEquals(actual.toString(), expected);
    }

    public void tryEvaluate(String expression, Type expectedType)
    {
        tryEvaluate(expression, expectedType, session);
    }

    private void tryEvaluate(String expression, Type expectedType, Session session)
    {
        selectUniqueValue(expression, expectedType, session, compiler);
    }

    public void tryEvaluateWithAll(String expression, Type expectedType)
    {
        tryEvaluateWithAll(expression, expectedType, session);
    }

    public void tryEvaluateWithAll(String expression, Type expectedType, Session session)
    {
        executeProjectionWithAll(expression, expectedType, session, compiler);
    }

    public void executeProjectionWithFullEngine(String projection)
    {
        MaterializedResult result = runner.execute("SELECT " + projection);
    }

    protected <T> T selectSingleValue(String projection, Type expectedType, Class<T> clazz)
    {
        Object object = selectSingleValue(projection, expectedType, compiler);
        assertEquals(object.getClass(), clazz);
        return (T) object;
    }

    private Object selectSingleValue(String projection, Type expectedType, ExpressionCompiler compiler)
    {
        return selectUniqueValue(projection, expectedType, session, compiler);
    }

    private Object selectUniqueValue(String projection, Type expectedType, Session session, ExpressionCompiler compiler)
    {
        List<Object> results = executeProjectionWithAll(projection, expectedType, session, compiler);
        HashSet<Object> resultSet = new HashSet<>(results);

        // we should only have a single result
        assertTrue(resultSet.size() == 1, "Expected only one result unique result, but got " + resultSet);

        return Iterables.getOnlyElement(resultSet);
    }

    // this is not safe as it catches all RuntimeExceptions
    @Deprecated
    public void assertInvalidFunction(String projection)
    {
        try {
            evaluateInvalid(projection);
            fail("Expected to fail");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    public void assertInvalidFunction(String projection, StandardErrorCode errorCode, String messagePattern)
    {
        try {
            evaluateInvalid(projection);
            fail("Expected to throw a PrestoException with message matching " + messagePattern);
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), errorCode.toErrorCode());
                assertTrue(e.getMessage().equals(messagePattern) || e.getMessage().matches(messagePattern), format("Error message [%s] doesn't match [%s]", e.getMessage(), messagePattern));
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertInvalidFunction(String projection, String messagePattern)
    {
        assertInvalidFunction(projection, INVALID_FUNCTION_ARGUMENT, messagePattern);
    }

    public void assertInvalidFunction(String projection, SemanticErrorCode expectedErrorCode)
    {
        try {
            evaluateInvalid(projection);
            fail(format("Expected to throw %s exception", expectedErrorCode));
        }
        catch (SemanticException e) {
            try {
                assertEquals(e.getCode(), expectedErrorCode);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertInvalidFunction(String projection, SemanticErrorCode expectedErrorCode, String message)
    {
        try {
            evaluateInvalid(projection);
            fail(format("Expected to throw %s exception", expectedErrorCode));
        }
        catch (SemanticException e) {
            try {
                assertEquals(e.getCode(), expectedErrorCode);
                assertEquals(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertInvalidFunction(String projection, ErrorCodeSupplier expectedErrorCode)
    {
        try {
            evaluateInvalid(projection);
            fail(format("Expected to throw %s exception", expectedErrorCode.toErrorCode()));
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), expectedErrorCode.toErrorCode());
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertNumericOverflow(String projection, String message)
    {
        try {
            evaluateInvalid(projection);
            fail("Expected to throw an NUMERIC_VALUE_OUT_OF_RANGE exception with message " + message);
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode());
                assertEquals(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertInvalidCast(String projection)
    {
        try {
            evaluateInvalid(projection);
            fail("Expected to throw an INVALID_CAST_ARGUMENT exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    public void assertInvalidCast(String projection, String message)
    {
        try {
            evaluateInvalid(projection);
            fail("Expected to throw an INVALID_CAST_ARGUMENT exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
                assertEquals(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    private void evaluateInvalid(String projection)
    {
        // type isn't necessary as the function is not valid
        selectSingleValue(projection, UNKNOWN, compiler);
    }

    public void assertCachedInstanceHasBoundedRetainedSize(String projection)
    {
        requireNonNull(projection, "projection is null");

        Expression projectionExpression = createExpression(session, projection, metadata, SYMBOL_TYPES);
        RowExpression projectionRowExpression = toRowExpression(session, projectionExpression);
        PageProcessor processor = compiler.compilePageProcessor(session.getSqlFunctionProperties(), Optional.empty(), ImmutableList.of(projectionRowExpression)).get();

        // This is a heuristic to detect whether the retained size of cachedInstance is bounded.
        // * The test runs at least 1000 iterations.
        // * The test passes if max retained size doesn't refresh after
        //   4x the number of iterations when max was last updated.
        // * The test fails if retained size reaches 1MB.
        // Note that 1MB is arbitrarily chosen and may be increased if a function implementation
        // legitimately needs more.

        long maxRetainedSize = 0;
        int maxIterationCount = 0;
        for (int iterationCount = 0; iterationCount < Math.max(1000, maxIterationCount * 4); iterationCount++) {
            Iterator<Optional<Page>> output = processor.process(
                    session.getSqlFunctionProperties(),
                    new DriverYieldSignal(),
                    newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                    SOURCE_PAGE);
            // consume the iterator
            Iterators.getOnlyElement(output);

            long retainedSize = processor.getProjections().stream()
                    .mapToLong(this::getRetainedSizeOfCachedInstance)
                    .sum();
            if (retainedSize > maxRetainedSize) {
                maxRetainedSize = retainedSize;
                maxIterationCount = iterationCount;
            }

            if (maxRetainedSize >= 1048576) {
                fail(format("The retained size of cached instance of function invocation is likely unbounded: %s", projection));
            }
        }
    }

    private long getRetainedSizeOfCachedInstance(PageProjectionWithOutputs projection)
    {
        Field[] fields = projection.getPageProjection().getClass().getDeclaredFields();
        long retainedSize = 0;
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = field.getName();
            if (!fieldName.startsWith("__cachedInstance")) {
                continue;
            }
            try {
                retainedSize += getRetainedSizeOf(field.get(projection));
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return retainedSize;
    }

    private long getRetainedSizeOf(Object object)
    {
        if (object instanceof PageBuilder) {
            return ((PageBuilder) object).getRetainedSizeInBytes();
        }
        if (object instanceof Block) {
            return ((Block) object).getRetainedSizeInBytes();
        }

        Class type = object.getClass();
        if (type.isArray()) {
            if (type == int[].class) {
                return sizeOf((int[]) object);
            }
            else if (type == boolean[].class) {
                return sizeOf((boolean[]) object);
            }
            else if (type == byte[].class) {
                return sizeOf((byte[]) object);
            }
            else if (type == long[].class) {
                return sizeOf((long[]) object);
            }
            else if (type == short[].class) {
                return sizeOf((short[]) object);
            }
            else if (type == Block[].class) {
                Object[] objects = (Object[]) object;
                return Arrays.stream(objects)
                        .mapToLong(this::getRetainedSizeOf)
                        .sum();
            }
            else {
                throw new IllegalArgumentException(format("Unknown type encountered: %s", type));
            }
        }

        long retainedSize = ClassLayout.parseClass(type).instanceSize();
        Field[] fields = type.getDeclaredFields();
        for (Field field : fields) {
            try {
                if (field.getType().isPrimitive() || Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                retainedSize += getRetainedSizeOf(field.get(object));
            }
            catch (IllegalAccessException t) {
                throw new RuntimeException(t);
            }
        }
        return retainedSize;
    }

    private List<Object> executeProjectionWithAll(String projection, Type expectedType, Session session, ExpressionCompiler compiler)
    {
        requireNonNull(projection, "projection is null");

        Expression projectionExpression = createExpression(session, projection, metadata, SYMBOL_TYPES);
        RowExpression projectionRowExpression = toRowExpression(session, projectionExpression);

        List<Object> results = new ArrayList<>();

        // If the projection does not need bound values, execute query using full engine
        if (!needsBoundValue(projectionExpression)) {
            MaterializedResult result = runner.execute("SELECT " + projection);
            assertType(result.getTypes(), expectedType);
            assertEquals(result.getTypes().size(), 1);
            assertEquals(result.getMaterializedRows().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedRows()).getField(0);
            results.add(queryResult);
        }

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(session.getSqlFunctionProperties(), Optional.empty(), projectionRowExpression, compiler);
        Object directOperatorValue = selectSingleValue(operatorFactory, expectedType, session);
        results.add(directOperatorValue);

        // interpret
        Object interpretedValue = interpret(projectionExpression, expectedType, session);
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(session.getSqlFunctionProperties(), Optional.empty(), projectionRowExpression, compiler);
        Object scanOperatorValue = selectSingleValue(scanProjectOperatorFactory, expectedType, createNormalSplit(), session);
        results.add(scanOperatorValue);

        // execute over record set
        Object recordValue = selectSingleValue(scanProjectOperatorFactory, expectedType, createRecordSetSplit(), session);
        results.add(recordValue);

        //
        // If the projection does not need bound values, execute query using full engine
        if (!needsBoundValue(projectionExpression)) {
            MaterializedResult result = runner.execute("SELECT " + projection);
            assertType(result.getTypes(), expectedType);
            assertEquals(result.getTypes().size(), 1);
            assertEquals(result.getMaterializedRows().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedRows()).getField(0);
            results.add(queryResult);
        }

        // validate type at end since some tests expect failure and for those UNKNOWN is used instead of actual type
        assertEquals(projectionRowExpression.getType(), expectedType);
        return results;
    }

    private RowExpression toRowExpression(Session session, Expression projectionExpression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                SQL_PARSER,
                SYMBOL_TYPES,
                projectionExpression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        return toRowExpression(projectionExpression, expressionTypes, INPUT_MAPPING);
    }

    private Object selectSingleValue(OperatorFactory operatorFactory, Type type, Session session)
    {
        Operator operator = operatorFactory.createOperator(createDriverContext(session));
        return selectSingleValue(operator, type);
    }

    private Object selectSingleValue(SourceOperatorFactory operatorFactory, Type type, Split split, Session session)
    {
        SourceOperator operator = operatorFactory.createOperator(createDriverContext(session));
        operator.addSplit(split);
        operator.noMoreSplits();
        return selectSingleValue(operator, type);
    }

    private Object selectSingleValue(Operator operator, Type type)
    {
        Page output = getAtMostOnePage(operator, SOURCE_PAGE);

        assertNotNull(output);
        assertEquals(output.getPositionCount(), 1);
        assertEquals(output.getChannelCount(), 1);

        Block block = output.getBlock(0);
        assertEquals(block.getPositionCount(), 1);

        return type.getObjectValue(session.getSqlFunctionProperties(), block, 0);
    }

    public void assertFilter(String filter, boolean expected, boolean withNoInputColumns)
    {
        assertFilter(filter, expected, withNoInputColumns, compiler);
    }

    private void assertFilter(String filter, boolean expected, boolean withNoInputColumns, ExpressionCompiler compiler)
    {
        List<Boolean> results = executeFilterWithAll(filter, TEST_SESSION, withNoInputColumns, compiler);
        HashSet<Boolean> resultSet = new HashSet<>(results);

        // we should only have a single result
        assertTrue(resultSet.size() == 1, "Expected only [" + expected + "] result unique result, but got " + resultSet);

        assertEquals((boolean) Iterables.getOnlyElement(resultSet), expected);
    }

    private List<Boolean> executeFilterWithAll(String filter, Session session, boolean executeWithNoInputColumns, ExpressionCompiler compiler)
    {
        requireNonNull(filter, "filter is null");

        Expression filterExpression = createExpression(session, filter, metadata, SYMBOL_TYPES);
        RowExpression filterRowExpression = toRowExpression(session, filterExpression);

        List<Boolean> results = new ArrayList<>();

        // execute as standalone operator
        OperatorFactory operatorFactory = compileFilterProject(session.getSqlFunctionProperties(), Optional.of(filterRowExpression), constant(true, BOOLEAN), compiler);
        results.add(executeFilter(operatorFactory, session));

        if (executeWithNoInputColumns) {
            // execute as standalone operator
            operatorFactory = compileFilterWithNoInputColumns(session.getSqlFunctionProperties(), filterRowExpression, compiler);
            results.add(executeFilterWithNoInputColumns(operatorFactory, session));
        }

        // interpret
        Boolean interpretedValue = (Boolean) interpret(filterExpression, BOOLEAN, session);
        if (interpretedValue == null) {
            interpretedValue = false;
        }
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(session.getSqlFunctionProperties(), Optional.of(filterRowExpression), constant(true, BOOLEAN), compiler);
        boolean scanOperatorValue = executeFilter(scanProjectOperatorFactory, createNormalSplit(), session);
        results.add(scanOperatorValue);

        // execute over record set
        boolean recordValue = executeFilter(scanProjectOperatorFactory, createRecordSetSplit(), session);
        results.add(recordValue);

        //
        // If the filter does not need bound values, execute query using full engine
        if (!needsBoundValue(filterExpression)) {
            MaterializedResult result = runner.execute("SELECT TRUE WHERE " + filter);
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

    public static Expression createExpression(String expression, Metadata metadata, TypeProvider symbolTypes)
    {
        return createExpression(TEST_SESSION, expression, metadata, symbolTypes);
    }

    public static Expression createExpression(Session session, String expression, Metadata metadata, TypeProvider symbolTypes)
    {
        Expression parsedExpression = SQL_PARSER.createExpression(expression, createParsingOptions(session));

        parsedExpression = rewriteIdentifiersToSymbolReferences(parsedExpression);

        final ExpressionAnalysis analysis = analyzeExpressions(
                session,
                metadata,
                SQL_PARSER,
                symbolTypes,
                ImmutableList.of(parsedExpression),
                ImmutableList.of(),
                WarningCollector.NOOP,
                false);

        Expression rewrittenExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);

                // cast expression if coercion is registered
                Type coercion = analysis.getCoercion(node);
                if (coercion != null) {
                    rewrittenExpression = new Cast(
                            rewrittenExpression,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(node));
                }

                return rewrittenExpression;
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isColumnReference(node)) {
                    return rewriteExpression(node, context, treeRewriter);
                }

                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);

                // cast expression if coercion is registered
                Type coercion = analysis.getCoercion(node);
                if (coercion != null) {
                    rewrittenExpression = new Cast(rewrittenExpression, coercion.getTypeSignature().toString());
                }

                return rewrittenExpression;
            }
        }, parsedExpression);

        return canonicalizeExpression(rewrittenExpression);
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

            assertTrue(BOOLEAN.getBoolean(page.getBlock(0), 0));
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
        final AtomicBoolean hasSymbolReferences = new AtomicBoolean();
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitSymbolReference(SymbolReference node, Void context)
            {
                hasSymbolReferences.set(true);
                return null;
            }
        }.process(projectionExpression, null);

        return hasSymbolReferences.get();
    }

    private Object interpret(Expression expression, Type expectedType, Session session)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, SQL_PARSER, SYMBOL_TYPES, expression, emptyList(), WarningCollector.NOOP);
        ExpressionInterpreter evaluator = ExpressionInterpreter.expressionInterpreter(expression, metadata, session, expressionTypes);

        Object result = evaluator.evaluate(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            int position = 0;
            int channel = INPUT_MAPPING.get(new VariableReferenceExpression(symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference())));
            Type type = SYMBOL_TYPES.get(symbol.toSymbolReference());

            Block block = SOURCE_PAGE.getBlock(channel);

            if (block.isNull(position)) {
                return null;
            }

            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                return type.getBoolean(block, position);
            }
            else if (javaType == long.class) {
                return type.getLong(block, position);
            }
            else if (javaType == double.class) {
                return type.getDouble(block, position);
            }
            else if (javaType == Slice.class) {
                return type.getSlice(block, position);
            }
            else if (javaType == Block.class) {
                return type.getObject(block, position);
            }
            else {
                throw new UnsupportedOperationException("not yet implemented");
            }
        });

        // convert result from stack type to Type ObjectValue
        Block block = Utils.nativeValueToBlock(expectedType, result);

        return expectedType.getObjectValue(session.getSqlFunctionProperties(), block, 0);
    }

    private static OperatorFactory compileFilterWithNoInputColumns(SqlFunctionProperties sqlFunctionProperties, RowExpression filter, ExpressionCompiler compiler)
    {
        try {
            Supplier<PageProcessor> processor = compiler.compilePageProcessor(sqlFunctionProperties, Optional.of(filter), ImmutableList.of());

            return new FilterAndProjectOperatorFactory(0, new PlanNodeId("test"), processor, ImmutableList.of(), new DataSize(0, BYTE), 0);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + filter + ": " + e.getMessage(), e);
        }
    }

    private static OperatorFactory compileFilterProject(SqlFunctionProperties sqlFunctionProperties, Optional<RowExpression> filter, RowExpression projection, ExpressionCompiler compiler)
    {
        try {
            Supplier<PageProcessor> processor = compiler.compilePageProcessor(sqlFunctionProperties, filter, ImmutableList.of(projection));
            return new FilterAndProjectOperatorFactory(0, new PlanNodeId("test"), processor, ImmutableList.of(projection.getType()), new DataSize(0, BYTE), 0);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + projection + ": " + e.getMessage(), e);
        }
    }

    private static SourceOperatorFactory compileScanFilterProject(SqlFunctionProperties sqlFunctionProperties, Optional<RowExpression> filter, RowExpression projection, ExpressionCompiler compiler)
    {
        try {
            Supplier<CursorProcessor> cursorProcessor = compiler.compileCursorProcessor(
                    sqlFunctionProperties,
                    filter,
                    ImmutableList.of(projection),
                    SOURCE_ID);

            Supplier<PageProcessor> pageProcessor = compiler.compilePageProcessor(
                    sqlFunctionProperties,
                    filter,
                    ImmutableList.of(projection));

            return new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    SOURCE_ID,
                    PAGE_SOURCE_PROVIDER,
                    cursorProcessor,
                    pageProcessor,
                    new TableHandle(
                            new ConnectorId("test"),
                            new ConnectorTableHandle() {},
                            new ConnectorTransactionHandle() {},
                            Optional.empty()),
                    ImmutableList.of(),
                    ImmutableList.of(projection.getType()),
                    Optional.empty(),
                    new DataSize(0, BYTE),
                    0);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling filter " + filter + ": " + e.getMessage(), e);
        }
    }

    private RowExpression toRowExpression(Expression projection, Map<NodeRef<Expression>, Type> expressionTypes, Map<VariableReferenceExpression, Integer> layout)
    {
        return translate(projection, expressionTypes, layout, metadata.getFunctionAndTypeManager(), session);
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
        return createTaskContext(EXECUTOR, SCHEDULED_EXECUTOR, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static void assertType(List<Type> types, Type expectedType)
    {
        assertTrue(types.size() == 1, "Expected one type, but got " + types);
        Type actualType = types.get(0);
        assertEquals(actualType, expectedType);
    }

    public Session getSession()
    {
        return session;
    }

    @Override
    public void close()
    {
        runner.close();
    }

    private static class TestPageSourceProvider
            implements PageSourceProvider
    {
        @Override
        public ConnectorPageSource createPageSource(Session session, Split split, TableHandle table, List<ColumnHandle> columns)
        {
            assertInstanceOf(split.getConnectorSplit(), FunctionAssertions.TestSplit.class);
            FunctionAssertions.TestSplit testSplit = (FunctionAssertions.TestSplit) split.getConnectorSplit();
            if (testSplit.isRecordSet()) {
                RecordSet records = InMemoryRecordSet.builder(ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BOOLEAN, BIGINT, VARCHAR, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, VARBINARY, INTEGER, TEST_ROW_TYPE))
                        .addRow(
                                1234L,
                                "hello",
                                12.34,
                                true,
                                new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis(),
                                "%el%",
                                null,
                                packDateTimeWithZone(new DateTime(1970, 1, 1, 0, 1, 0, 999, DateTimeZone.UTC).getMillis(), TimeZoneKey.getTimeZoneKey("Z")),
                                Slices.wrappedBuffer((byte) 0xab),
                                1234,
                                TEST_ROW_DATA.getBlock(0))
                        .build();
                return new RecordPageSource(records);
            }
            else {
                return new FixedPageSource(ImmutableList.of(SOURCE_PAGE));
            }
        }
    }

    private static Split createRecordSetSplit()
    {
        return new Split(new ConnectorId("test"), TestingTransactionHandle.create(), new TestSplit(true));
    }

    private static Split createNormalSplit()
    {
        return new Split(new ConnectorId("test"), TestingTransactionHandle.create(), new TestSplit(false));
    }

    private static RowType createTestRowType(int numberOfFields)
    {
        Iterator<Type> types = Iterables.<Type>cycle(
                BIGINT,
                INTEGER,
                VARCHAR,
                DOUBLE,
                BOOLEAN,
                VARBINARY,
                RowType.from(ImmutableList.of(RowType.field("nested_nested_column", VARCHAR)))).iterator();

        List<RowType.Field> fields = new ArrayList<>();
        for (int fieldIdx = 0; fieldIdx < numberOfFields; fieldIdx++) {
            fields.add(new RowType.Field(Optional.of("nested_column_" + fieldIdx), types.next()));
        }

        return RowType.from(fields);
    }

    private static Block createTestRowData(RowType rowType)
    {
        Iterator<Object> values = Iterables.cycle(
                1234L,
                34,
                "hello",
                12.34d,
                true,
                Slices.wrappedBuffer((byte) 0xab),
                createRowBlock(ImmutableList.of(VARCHAR), Collections.singleton("innerFieldValue").toArray()).getBlock(0)).iterator();

        final int numFields = rowType.getFields().size();
        Object[] rowValues = new Object[numFields];
        for (int fieldIdx = 0; fieldIdx < numFields; fieldIdx++) {
            rowValues[fieldIdx] = values.next();
        }

        return createRowBlock(rowType.getTypeParameters(), rowValues);
    }

    private static class TestSplit
            implements ConnectorSplit
    {
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
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
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
