/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.OperatorFactory;
import com.facebook.presto.sql.planner.InterpretedProjectionFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.util.LocalQueryRunner.createDualLocalQueryRunner;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class FunctionAssertions
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");

    private static final ExpressionCompiler compiler = new ExpressionCompiler(new MetadataManager());

    public static final Operator SOURCE = createOperator(new Page(
            createLongsBlock(1234L),
            createStringsBlock("hello"),
            createDoublesBlock(12.34),
            createBooleansBlock(true),
            createLongsBlock(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
            createStringsBlock("%el%"),
            createStringsBlock((String) null)));

    private static final Map<Input, Type> INPUT_TYPES = ImmutableMap.<Input, Type>builder()
            .put(new Input(0, 0), Type.LONG)
            .put(new Input(1, 0), Type.STRING)
            .put(new Input(2, 0), Type.DOUBLE)
            .put(new Input(3, 0), Type.BOOLEAN)
            .put(new Input(4, 0), Type.LONG)
            .put(new Input(5, 0), Type.STRING)
            .put(new Input(6, 0), Type.STRING)
            .build();

    private static final Map<Symbol, Input> INPUT_MAPPING = ImmutableMap.<Symbol, Input>builder()
            .put(new Symbol("bound_long"), new Input(0, 0))
            .put(new Symbol("bound_string"), new Input(1, 0))
            .put(new Symbol("bound_double"), new Input(2, 0))
            .put(new Symbol("bound_boolean"), new Input(3, 0))
            .put(new Symbol("bound_timestamp"), new Input(4, 0))
            .put(new Symbol("bound_pattern"), new Input(5, 0))
            .put(new Symbol("bound_null_string"), new Input(6, 0))
            .build();

    private FunctionAssertions() {}

    public static void assertFunction(String projection, Object expected)
    {
        if (expected instanceof Integer) {
            expected = ((Integer) expected).longValue();
        }
        else if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(Charsets.UTF_8);
        }
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunctionNull(String projection)
    {
        assertNull(selectSingleValue(projection));
    }

    public static Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner(), SESSION);
    }

    public static Object selectSingleValue(String projection, Session session)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner(session), session);
    }

    public static Object selectCompiledSingleValue(String projection)
    {
        checkNotNull(projection, "projection is null");

        // compile
        Operator compiledOperator = createCompiledOperatorFactory(projection).createOperator(SOURCE, SESSION);
        return execute(compiledOperator);
    }

    public static Object selectInterpretedSingleValue(String projection, Type expressionType)
    {
        checkNotNull(projection, "projection is null");

        // compile
        FilterAndProjectOperator interpretedOperator = createInterpretedOperator(projection, expressionType, SESSION);
        return execute(interpretedOperator);
    }

    public static void assertFilter(String expression, boolean expected)
    {
        assertFilter(expression, expected, SESSION);
    }

    public static void assertFilter(String expression, boolean expected, Session session)
    {
        Expression parsedExpression = FunctionAssertions.parseExpression(expression);

        OperatorFactory operatorFactory;
        try {
            operatorFactory = compiler.compileFilterAndProjectOperator(parsedExpression, ImmutableList.<Expression>of(TRUE_LITERAL), INPUT_TYPES);
        }
        catch (Throwable e) {
            throw new RuntimeException("Error compiling " + expression, e);
        }

        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_VARBINARY, SINGLE_DOUBLE, SINGLE_BOOLEAN, SINGLE_LONG, SINGLE_VARBINARY, SINGLE_VARBINARY).row(
                1234L,
                "hello",
                12.34,
                true,
                MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
                "%el%",
                null
        ).build();

        Operator source = createOperator(input);
        Operator operator = operatorFactory.createOperator(source, session);

        PageIterator pageIterator = operator.iterator(new OperatorStats());

        boolean value;
        if (pageIterator.hasNext()) {
            Page page = pageIterator.next();
            assertEquals(page.getPositionCount(), 1);
            assertEquals(page.getChannelCount(), 1);

            BlockCursor cursor = page.getBlock(0).cursor();
            assertTrue(cursor.advanceNextPosition());
            assertTrue(cursor.getBoolean(0));
            value = true;
        } else {
            value = false;
        }
        assertEquals(value, expected);
    }

    private static Object selectSingleValue(String projection, LocalQueryRunner runner, Session session)
    {
        checkNotNull(projection, "projection is null");

        // compile operator
        OperatorFactory operatorFactory = createCompiledOperatorFactory(projection);

        // execute using table scan over plain old operator
        Operator operator = operatorFactory.createOperator(createTableScanOperator(SOURCE), session);
        Object directOperatorValue = execute(operator);
        Type expressionType = Type.fromRaw(operator.getTupleInfos().get(0).getTypes().get(0));

        // execute using table scan over record set
        Object recordValue = execute(operatorFactory.createOperator(createTableScanOperator(createRecordProjectOperator()), session));
        assertEquals(recordValue, directOperatorValue);

        // interpret
        FilterAndProjectOperator interpretedOperator = createInterpretedOperator(projection, expressionType, session);
        Object interpretedValue = execute(interpretedOperator);

        // verify interpreted and compiled value are the same
        assertEquals(interpretedValue, directOperatorValue);

        //
        // If the projection does not need bound values, execute query using full engine
        boolean needsBoundValues = parseExpression(projection).accept(new AstVisitor<Boolean, Object>()
        {
            @Override
            public Boolean visitInputReference(InputReference node, Object context)
            {
                return true;
            }
        }, null) == Boolean.TRUE;

        if (!needsBoundValues) {
            MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");
            assertEquals(result.getTupleInfo().getFieldCount(), 1);
            assertEquals(result.getMaterializedTuples().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedTuples()).getField(0);
            assertEquals(directOperatorValue, queryResult);
        }

        return directOperatorValue;
    }

    public static FilterAndProjectOperator createInterpretedOperator(String projection, Type expressionType)
    {
        return createInterpretedOperator(projection, expressionType, SESSION);
    }

    public static FilterAndProjectOperator createInterpretedOperator(String projection, Type expressionType, Session session)
    {
        ProjectionFunction projectionFunction = new InterpretedProjectionFunction(expressionType,
                createExpression(projection),
                INPUT_MAPPING,
                new MetadataManager(),
                session,
                INPUT_TYPES);

        return new FilterAndProjectOperator(SOURCE, FilterFunctions.TRUE_FUNCTION, ImmutableList.of(projectionFunction));
    }

    public static OperatorFactory createCompiledOperatorFactory(String projection)
    {
        Expression parsedExpression = parseExpression(projection);

        // compile and execute
        OperatorFactory operatorFactory;
        try {
            operatorFactory = compiler.compileFilterAndProjectOperator(TRUE_LITERAL, ImmutableList.of(parsedExpression), INPUT_TYPES);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + parsedExpression + ": " + e.getMessage(), e);
        }
        return operatorFactory;
    }

    public static Object execute(Operator operator)
    {
        OperatorStats operatorStats = new OperatorStats();

        PageIterator pageIterator = operator.iterator(operatorStats);
        assertTrue(pageIterator.hasNext());
        Page page = pageIterator.next();
        assertFalse(pageIterator.hasNext());

        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getChannelCount(), 1);

        SplitExecutionStats snapshot = operatorStats.snapshot();
        assertEquals(snapshot.getCompletedPositions().getTotalCount(), 1);
        assertGreaterThan(snapshot.getCompletedDataSize().getTotalCount(), 1L);

        return getSingleCellValue(page.getBlock(0));
    }

    public static Object execute(ProjectionFunction projectionFunction)
    {
        RecordCursor cursor = createRecordProjectOperator().cursor();
        assertTrue(cursor.advanceNextPosition());

        BlockBuilder output = new BlockBuilder(projectionFunction.getTupleInfo());
        projectionFunction.project(cursor, output);

        assertFalse(cursor.advanceNextPosition());

        return getSingleCellValue(output.build());
    }

    private static Object getSingleCellValue(Block block)
    {
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getTupleInfo().getFieldCount(), 1);

        BlockCursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        if (cursor.isNull(0)) {
            return null;
        }
        else {
            return cursor.getTuple().toValues().get(0);
        }
    }

    public static Expression parseExpression(String expression)
    {
        Expression parsedExpression = createExpression(expression);

        parsedExpression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), parsedExpression);
        return parsedExpression;
    }

    public static RecordProjectOperator createRecordProjectOperator()
    {
        return new RecordProjectOperator(new InMemoryRecordSet(
                ImmutableList.of(
                        ColumnType.LONG,
                        ColumnType.STRING,
                        ColumnType.DOUBLE,
                        ColumnType.BOOLEAN,
                        ColumnType.LONG,
                        ColumnType.STRING,
                        ColumnType.STRING),
                ImmutableList.of(Arrays.<Object>asList(
                        1234L,
                        "hello",
                        12.34,
                        true,
                        MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
                        "%el%",
                        null))));
    }

    public static TableScanOperator createTableScanOperator(final Operator source)
    {
        TableScanOperator tableScanOperator = new TableScanOperator(
                new DataStreamProvider()
                {
                    @Override
                    public Operator createDataStream(Split split, List<ColumnHandle> columns)
                    {
                        return source;
                    }
                },
                source.getTupleInfos(),
                ImmutableList.<ColumnHandle>of());

        tableScanOperator.addSplit(new Split()
        {
            @Override
            public boolean isRemotelyAccessible()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<HostAddress> getAddresses()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getInfo()
            {
                throw new UnsupportedOperationException();
            }
        });

        return tableScanOperator;
    }
}
