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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields.declareCommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.collectCSEByLevel;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCursorProcessorCompiler
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_MANAGER = METADATA.getFunctionAndTypeManager();

    private static final CallExpression ADD_X_Y = call(
            ADD.name(),
            FUNCTION_MANAGER.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
            BIGINT,
            field(0, BIGINT),
            field(1, BIGINT));

    private static final CallExpression ADD_X_Y_GREATER_THAN_2 = call(
            GREATER_THAN.name(),
            FUNCTION_MANAGER.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
            BOOLEAN,
            ADD_X_Y,
            constant(2L, BIGINT));

    private static final CallExpression ADD_X_Y_LESS_THAN_10 = call(
            LESS_THAN.name(),
            FUNCTION_MANAGER.resolveOperator(LESS_THAN, fromTypes(BIGINT, BIGINT)),
            BOOLEAN,
            ADD_X_Y,
            constant(10L, BIGINT));

    private static final CallExpression ADD_X_Y_Z = call(
            ADD.name(),
            FUNCTION_MANAGER.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
            BIGINT,
            call(
                    ADD.name(),
                    FUNCTION_MANAGER.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                    BIGINT,
                    field(0, BIGINT),
                    field(1, BIGINT)),
            field(2, BIGINT));

    @Test
    public void testRewriteRowExpressionWithCSE()
    {
        CursorProcessorCompiler cseCursorCompiler = new CursorProcessorCompiler(METADATA, true, emptyMap());

        ClassDefinition cursorProcessorClassDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(CursorProcessor.class.getSimpleName()),
                type(Object.class),
                type(CursorProcessor.class));

        RowExpression filter = new SpecialFormExpression(AND, BIGINT, ADD_X_Y_GREATER_THAN_2);
        List<RowExpression> projections = ImmutableList.of(ADD_X_Y_Z);
        List<RowExpression> rowExpressions = ImmutableList.<RowExpression>builder()
                .addAll(projections)
                .add(filter)
                .build();
        Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel = collectCSEByLevel(rowExpressions);

        Map<VariableReferenceExpression, CommonSubExpressionRewriter.CommonSubExpressionFields> cseFields = declareCommonSubExpressionFields(cursorProcessorClassDefinition, commonSubExpressionsByLevel);
        Map<RowExpression, VariableReferenceExpression> commonSubExpressions = commonSubExpressionsByLevel.values().stream()
                .flatMap(m -> m.entrySet().stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        // X+Y as CSE
        assertEquals(1, cseFields.size());
        VariableReferenceExpression cseVariable = cseFields.keySet().iterator().next();

        RowExpression rewrittenFilter = cseCursorCompiler.rewriteRowExpressionsWithCSE(ImmutableList.of(filter), commonSubExpressions).get(0);

        List<RowExpression> rewrittenProjections = cseCursorCompiler.rewriteRowExpressionsWithCSE(projections, commonSubExpressions);

        // X+Y+Z contains CSE X+Y
        assertTrue(((CallExpression) rewrittenProjections.get(0)).getArguments().contains(cseVariable));

        // X+Y > 2 consists CSE X+Y
        assertTrue(((CallExpression) ((SpecialFormExpression) rewrittenFilter).getArguments().get(0)).getArguments().contains(cseVariable));
    }

    @Test
    public void testCompilerWithCSE()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(METADATA, 0);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(METADATA, functionCompiler);

        RowExpression filter = new SpecialFormExpression(AND, BIGINT, ADD_X_Y_GREATER_THAN_2, ADD_X_Y_LESS_THAN_10);
        List<? extends RowExpression> projections = createIfProjectionList(5);

        Supplier<CursorProcessor> cseCursorProcessorSupplier = expressionCompiler.compileCursorProcessor(SESSION.getSqlFunctionProperties(), Optional.of(filter), projections, "key", true);
        Supplier<CursorProcessor> noCseSECursorProcessorSupplier = expressionCompiler.compileCursorProcessor(SESSION.getSqlFunctionProperties(), Optional.of(filter), projections, "key", false);

        Page input = createLongBlockPage(2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        PageBuilder pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(toList()));
        RecordSet recordSet = new PageRecordSet(types, input);
        cseCursorProcessorSupplier.get().process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), recordSet.cursor(), pageBuilder);

        Page pageFromCSE = pageBuilder.build();
        pageBuilder.reset();

        noCseSECursorProcessorSupplier.get().process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), recordSet.cursor(), pageBuilder);
        Page pageFromNoCSE = pageBuilder.build();

        checkPageEqual(pageFromCSE, pageFromNoCSE);
    }

    @Test
    public void testLargeProjectionBatching()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(METADATA, 0);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(METADATA, functionCompiler);

        int projectionCount = 1500;
        List<RowExpression> projections = IntStream.range(0, projectionCount)
                .mapToObj(i -> field(i % 2, BIGINT))
                .collect(toImmutableList());

        Supplier<CursorProcessor> cursorProcessorSupplier = expressionCompiler.compileCursorProcessor(
                SESSION.getSqlFunctionProperties(),
                Optional.empty(), // No filter
                projections,
                "testLargeProjection",
                false);

        CursorProcessor processor = cursorProcessorSupplier.get();
        assertTrue(processor != null, "CursorProcessor should be created successfully for large projections");

        Page input = createLongBlockPage(2, 1L, 2L, 3L, 4L, 5L);
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        PageBuilder pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(toList()));
        RecordSet recordSet = new PageRecordSet(types, input);

        processor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), recordSet.cursor(), pageBuilder);
        Page result = pageBuilder.build();

        assertEquals(result.getChannelCount(), projectionCount, "Result should have all projected columns");
        assertEquals(result.getPositionCount(), input.getPositionCount(), "Result should have same number of rows as input");
    }

    @Test
    public void testProjectionBatchBoundary()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(METADATA, 0);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(METADATA, functionCompiler);

        int projectionCount = 1000;
        List<RowExpression> projections = IntStream.range(0, projectionCount)
                .mapToObj(i -> field(i % 2, BIGINT))
                .collect(toImmutableList());

        Supplier<CursorProcessor> cursorProcessorSupplier = expressionCompiler.compileCursorProcessor(
                SESSION.getSqlFunctionProperties(),
                Optional.empty(),
                projections,
                "testBatchBoundary",
                false);

        CursorProcessor processor = cursorProcessorSupplier.get();
        assertTrue(processor != null, "CursorProcessor should be created successfully at batch boundary");

        Page input = createLongBlockPage(2, 10L, 20L);
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        PageBuilder pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(toList()));
        RecordSet recordSet = new PageRecordSet(types, input);

        processor.process(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), recordSet.cursor(), pageBuilder);
        Page result = pageBuilder.build();

        assertEquals(result.getChannelCount(), projectionCount);
        assertEquals(result.getPositionCount(), input.getPositionCount());
    }

    private static Page createLongBlockPage(int blockCount, long... values)
    {
        Block[] blocks = new Block[blockCount];
        for (int i = 0; i < blockCount; i++) {
            BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
            for (long value : values) {
                BIGINT.writeLong(builder, value);
            }
            blocks[i] = builder.build();
        }
        return new Page(blocks);
    }

    private List<? extends RowExpression> createIfProjectionList(int projectionCount)
    {
        return IntStream.range(0, projectionCount)
                .mapToObj(i -> new SpecialFormExpression(
                        IF,
                        BIGINT,
                        call(
                                GREATER_THAN.name(),
                                FUNCTION_MANAGER.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                                BOOLEAN,
                                ADD_X_Y,
                                constant(8L, BIGINT)),
                        constant((long) i, BIGINT),
                        constant((long) i + 1, BIGINT)))
                .collect(toImmutableList());
    }

    private void checkBlockEqual(Block a, Block b)
    {
        assertEquals(a.getPositionCount(), b.getPositionCount());
        for (int i = 0; i < a.getPositionCount(); i++) {
            assertEquals(a.getLong(i), b.getLong(i));
        }
    }

    private void checkPageEqual(Page a, Page b)
    {
        assertEquals(a.getPositionCount(), b.getPositionCount());
        assertEquals(a.getChannelCount(), b.getChannelCount());
        for (int i = 0; i < a.getChannelCount(); i++) {
            checkBlockEqual(a.getBlock(i), b.getBlock(i));
        }
    }
}
