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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPageFunctionCompiler
{
    private static final FunctionAndTypeManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();

    private static final CallExpression ADD_10_EXPRESSION = call(
            ADD.name(),
            FUNCTION_MANAGER.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
            BIGINT,
            field(0, BIGINT),
            constant(10L, BIGINT));

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
    public void testFailureDoesNotCorruptFutureResults()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);

        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty());
        PageProjection projection = projectionSupplier.get();

        // process good page and verify we got the expected number of result rows
        Page goodPage = createLongBlockPage(1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Block goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount())).get(0);
        assertEquals(goodPage.getPositionCount(), goodResult.getPositionCount());

        // addition will throw due to integer overflow
        Page badPage = createLongBlockPage(1, 0, 1, 2, 3, 4, Long.MAX_VALUE);
        try {
            project(projection, badPage, SelectedPositions.positionsRange(0, 100));
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode());
        }

        // running the good page should still work
        // if block builder in generated code was not reset properly, we could get junk results after the failure
        goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount())).get(0);
        assertEquals(goodPage.getPositionCount(), goodResult.getPositionCount());
    }

    @Test
    public void testGeneratedClassName()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);

        String planNodeId = "7";
        String stageId = "20170707_223500_67496_zguwn.2";
        String classSuffix = stageId + "_" + planNodeId;
        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of(classSuffix));
        PageProjection projection = projectionSupplier.get();
        Work<List<Block>> work = projection.project(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), createLongBlockPage(1, 0), SelectedPositions.positionsRange(0, 1));
        // class name should look like PageProjectionOutput_20170707_223500_67496_zguwn_2_7_XX
        assertTrue(work.getClass().getSimpleName().startsWith("PageProjectionWork_" + stageId.replace('.', '_') + "_" + planNodeId));
    }

    @Test
    public void testCache()
    {
        PageFunctionCompiler cacheCompiler = new PageFunctionCompiler(createTestMetadataManager(), 100);
        assertSame(
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()),
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()));
        assertSame(
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")),
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")));
        assertSame(
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")),
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint2")));
        assertSame(
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()),
                cacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint2")));

        PageFunctionCompiler noCacheCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);
        assertNotSame(
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()),
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()));
        assertNotSame(
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")),
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")));
        assertNotSame(
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint")),
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint2")));
        assertNotSame(
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.empty()),
                noCacheCompiler.compileProjection(SESSION.getSqlFunctionProperties(), ADD_10_EXPRESSION, Optional.of("hint2")));
    }

    @Test
    public void testCommonSubExpressionInProjection()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);

        List<Supplier<PageProjectionWithOutputs>> pageProjectionsCSE = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), ImmutableList.of(ADD_X_Y, ADD_X_Y_Z), true, Optional.empty());
        assertEquals(pageProjectionsCSE.size(), 1);
        List<Supplier<PageProjectionWithOutputs>> pageProjectionsNoCSE = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), ImmutableList.of(ADD_X_Y, ADD_X_Y_Z), false, Optional.empty());
        assertEquals(pageProjectionsNoCSE.size(), 2);

        Page input = createLongBlockPage(3, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Block> cseResult = project(pageProjectionsCSE.get(0).get().getPageProjection(), input, SelectedPositions.positionsRange(0, input.getPositionCount()));
        assertEquals(cseResult.size(), 2);
        List<Block> noCseResult1 = project(pageProjectionsNoCSE.get(0).get().getPageProjection(), input, SelectedPositions.positionsRange(0, input.getPositionCount()));
        assertEquals(noCseResult1.size(), 1);
        List<Block> noCseResult2 = project(pageProjectionsNoCSE.get(1).get().getPageProjection(), input, SelectedPositions.positionsRange(0, input.getPositionCount()));
        assertEquals(noCseResult2.size(), 1);
        checkBlockEqual(cseResult.get(0), noCseResult1.get(0));
        checkBlockEqual(cseResult.get(1), noCseResult2.get(0));
    }

    @Test
    public void testCommonSubExpressionLongProjectionList()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);

        List<Supplier<PageProjectionWithOutputs>> pageProjections = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), createIfProjectionList(5), true, Optional.empty());
        assertEquals(pageProjections.size(), 1);

        pageProjections = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), createIfProjectionList(10), true, Optional.empty());
        assertEquals(pageProjections.size(), 1);

        pageProjections = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), createIfProjectionList(11), true, Optional.empty());
        assertEquals(pageProjections.size(), 2);

        pageProjections = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), createIfProjectionList(20), true, Optional.empty());
        assertEquals(pageProjections.size(), 2);

        pageProjections = functionCompiler.compileProjections(SESSION.getSqlFunctionProperties(), createIfProjectionList(101), true, Optional.empty());
        assertEquals(pageProjections.size(), 11);
    }

    @Test
    public void testCommonSubExpressionInFilter()
    {
        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(createTestMetadataManager(), 0);

        Supplier<PageFilter> pageFilter = functionCompiler.compileFilter(SESSION.getSqlFunctionProperties(), new SpecialFormExpression(AND, BIGINT, ADD_X_Y_GREATER_THAN_2, ADD_X_Y_LESS_THAN_10), true, Optional.empty());

        Page input = createLongBlockPage(2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        SelectedPositions positions = filter(pageFilter.get(), input);
        assertEquals(positions.size(), 3);
        assertEquals(positions.getPositions(), new int[]{2, 3, 4});
    }

    private void checkBlockEqual(Block a, Block b)
    {
        assertEquals(a.getPositionCount(), b.getPositionCount());
        for (int i = 0; i < a.getPositionCount(); i++) {
            assertEquals(a.getLong(i), b.getLong(i));
        }
    }

    private List<Block> project(PageProjection projection, Page page, SelectedPositions selectedPositions)
    {
        Work<List<Block>> work = projection.project(SESSION.getSqlFunctionProperties(), new DriverYieldSignal(), page, selectedPositions);
        assertTrue(work.process());
        return work.getResult();
    }

    private SelectedPositions filter(PageFilter filter, Page page)
    {
        return filter.filter(SESSION.getSqlFunctionProperties(), filter.getInputChannels().getInputChannels(page));
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
                                field(0, BIGINT),
                                constant(10L, BIGINT)),
                        constant((long) i, BIGINT),
                        constant((long) i + 1, BIGINT)))
                .collect(toImmutableList());
    }
}
