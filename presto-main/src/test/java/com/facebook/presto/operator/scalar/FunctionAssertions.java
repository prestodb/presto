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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.InterpretedPageFilter;
import com.facebook.presto.operator.project.InterpretedPageProjection;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProcessorOutput;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.analyzer.ExpressionAnalysis;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
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
import com.facebook.presto.type.TypeRegistry;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.createTimestampsWithTimezoneBlock;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeExpressionsWithSymbols;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.SqlToRowExpressionTranslator.translate;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
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
            createIntsBlock(1234));

    private static final Page ZERO_CHANNEL_PAGE = new Page(1);

    private static final Map<Integer, Type> INPUT_TYPES = ImmutableMap.<Integer, Type>builder()
            .put(0, BIGINT)
            .put(1, VARCHAR)
            .put(2, DOUBLE)
            .put(3, BOOLEAN)
            .put(4, BIGINT)
            .put(5, VARCHAR)
            .put(6, VARCHAR)
            .put(7, TIMESTAMP_WITH_TIME_ZONE)
            .put(8, VARBINARY)
            .put(9, INTEGER)
            .build();

    private static final Map<Symbol, Integer> INPUT_MAPPING = ImmutableMap.<Symbol, Integer>builder()
            .put(new Symbol("bound_long"), 0)
            .put(new Symbol("bound_string"), 1)
            .put(new Symbol("bound_double"), 2)
            .put(new Symbol("bound_boolean"), 3)
            .put(new Symbol("bound_timestamp"), 4)
            .put(new Symbol("bound_pattern"), 5)
            .put(new Symbol("bound_null_string"), 6)
            .put(new Symbol("bound_timestamp_with_timezone"), 7)
            .put(new Symbol("bound_binary_literal"), 8)
            .put(new Symbol("bound_integer"), 9)
            .build();

    private static final TypeProvider SYMBOL_TYPES = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("bound_long"), BIGINT)
            .put(new Symbol("bound_string"), VARCHAR)
            .put(new Symbol("bound_double"), DOUBLE)
            .put(new Symbol("bound_boolean"), BOOLEAN)
            .put(new Symbol("bound_timestamp"), BIGINT)
            .put(new Symbol("bound_pattern"), VARCHAR)
            .put(new Symbol("bound_null_string"), VARCHAR)
            .put(new Symbol("bound_timestamp_with_timezone"), TIMESTAMP_WITH_TIME_ZONE)
            .put(new Symbol("bound_binary_literal"), VARBINARY)
            .put(new Symbol("bound_integer"), INTEGER)
            .build());

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

    public TypeRegistry getTypeRegistry()
    {
        return runner.getTypeManager();
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public FunctionAssertions addFunctions(List<? extends SqlFunction> functionInfos)
    {
        metadata.addFunctions(functionInfos);
        return this;
    }

    public FunctionAssertions addScalarFunctions(Class<?> clazz)
    {
        metadata.addFunctions(new FunctionListBuilder().scalars(clazz).getFunctions());
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
        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(projectionRowExpression)).get();

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
            PageProcessorOutput output = processor.process(session.toConnectorSession(), new DriverYieldSignal(), SOURCE_PAGE);
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

    private long getRetainedSizeOfCachedInstance(PageProjection projection)
    {
        Field[] fields = projection.getClass().getDeclaredFields();
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
        OperatorFactory operatorFactory = compileFilterProject(Optional.empty(), projectionRowExpression, compiler);
        Object directOperatorValue = selectSingleValue(operatorFactory, expectedType, session);
        results.add(directOperatorValue);

        // interpret
        Operator interpretedFilterProject = interpretedFilterProject(Optional.empty(), projectionExpression, expectedType, session);
        Object interpretedValue = selectSingleValue(interpretedFilterProject, expectedType);
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(Optional.empty(), projectionRowExpression, compiler);
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
        Expression translatedProjection = new SymbolToInputRewriter(INPUT_MAPPING).rewrite(projectionExpression);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(
                session,
                metadata,
                SQL_PARSER,
                INPUT_TYPES,
                ImmutableList.of(translatedProjection),
                ImmutableList.of());
        return toRowExpression(translatedProjection, expressionTypes);
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

        return type.getObjectValue(session.toConnectorSession(), block, 0);
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
        OperatorFactory operatorFactory = compileFilterProject(Optional.of(filterRowExpression), constant(true, BOOLEAN), compiler);
        results.add(executeFilter(operatorFactory, session));

        if (executeWithNoInputColumns) {
            // execute as standalone operator
            operatorFactory = compileFilterWithNoInputColumns(filterRowExpression, compiler);
            results.add(executeFilterWithNoInputColumns(operatorFactory, session));
        }

        // interpret
        boolean interpretedValue = executeFilter(interpretedFilterProject(Optional.of(filterExpression), TRUE_LITERAL, BOOLEAN, session));
        results.add(interpretedValue);

        // execute over normal operator
        SourceOperatorFactory scanProjectOperatorFactory = compileScanFilterProject(Optional.of(filterRowExpression), constant(true, BOOLEAN), compiler);
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

        final ExpressionAnalysis analysis = analyzeExpressionsWithSymbols(
                session,
                metadata,
                SQL_PARSER,
                symbolTypes,
                ImmutableList.of(parsedExpression),
                ImmutableList.of(),
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

    private Operator interpretedFilterProject(Optional<Expression> filter, Expression projection, Type expectedType, Session session)
    {
        Optional<PageFilter> pageFilter = filter
                .map(expression -> new InterpretedPageFilter(
                        expression,
                        SYMBOL_TYPES,
                        INPUT_MAPPING,
                        metadata,
                        SQL_PARSER,
                        session));

        PageProjection pageProjection = new InterpretedPageProjection(projection, SYMBOL_TYPES, INPUT_MAPPING, metadata, SQL_PARSER, session);
        assertEquals(pageProjection.getType(), expectedType);

        PageProcessor processor = new PageProcessor(pageFilter, ImmutableList.of(pageProjection));
        OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                () -> processor,
                ImmutableList.of(pageProjection.getType()),
                new DataSize(0, BYTE),
                0);
        return operatorFactory.createOperator(createDriverContext(session));
    }

    private static OperatorFactory compileFilterWithNoInputColumns(RowExpression filter, ExpressionCompiler compiler)
    {
        try {
            Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of());

            return new FilterAndProjectOperatorFactory(0, new PlanNodeId("test"), processor, ImmutableList.of(), new DataSize(0, BYTE), 0);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + filter + ": " + e.getMessage(), e);
        }
    }

    private static OperatorFactory compileFilterProject(Optional<RowExpression> filter, RowExpression projection, ExpressionCompiler compiler)
    {
        try {
            Supplier<PageProcessor> processor = compiler.compilePageProcessor(filter, ImmutableList.of(projection));
            return new FilterAndProjectOperatorFactory(0, new PlanNodeId("test"), processor, ImmutableList.of(projection.getType()), new DataSize(0, BYTE), 0);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + projection + ": " + e.getMessage(), e);
        }
    }

    private static SourceOperatorFactory compileScanFilterProject(Optional<RowExpression> filter, RowExpression projection, ExpressionCompiler compiler)
    {
        try {
            Supplier<CursorProcessor> cursorProcessor = compiler.compileCursorProcessor(
                    filter,
                    ImmutableList.of(projection),
                    SOURCE_ID);

            Supplier<PageProcessor> pageProcessor = compiler.compilePageProcessor(
                    filter,
                    ImmutableList.of(projection));

            return new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    SOURCE_ID,
                    PAGE_SOURCE_PROVIDER,
                    cursorProcessor,
                    pageProcessor,
                    ImmutableList.of(),
                    ImmutableList.of(projection.getType()),
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

    private RowExpression toRowExpression(Expression projection, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return translate(projection, SCALAR, expressionTypes, metadata.getFunctionRegistry(), metadata.getTypeManager(), session, false);
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
                .addPipelineContext(0, true, true)
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
        public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
        {
            assertInstanceOf(split.getConnectorSplit(), FunctionAssertions.TestSplit.class);
            FunctionAssertions.TestSplit testSplit = (FunctionAssertions.TestSplit) split.getConnectorSplit();
            if (testSplit.isRecordSet()) {
                RecordSet records = InMemoryRecordSet.builder(ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BOOLEAN, BIGINT, VARCHAR, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, VARBINARY, INTEGER))
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
                                1234)
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
