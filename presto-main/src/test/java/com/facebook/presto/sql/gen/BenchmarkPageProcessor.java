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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.TestPageProcessor;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.operator.project.SelectedPositions.positionsList;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(5)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkPageProcessor
{
    @Benchmark
    public Page handCoded(BenchmarkData data)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE));
        int count = data.handcodedProcessor.process(data.inputPage, 0, data.inputPage.getPositionCount(), pageBuilder);
        checkState(count == data.inputPage.getPositionCount());
        return pageBuilder.build();
    }

    @Test
    public void verifyHandCoded()
    {
        BenchmarkData benchmarkData = new BenchmarkData();
        benchmarkData.setup();
        BenchmarkPageProcessor benchmarkPageProcessor = new BenchmarkPageProcessor();
        benchmarkPageProcessor.handCoded(benchmarkData);
    }

    @Benchmark
    public List<Optional<Page>> compiled(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.compiledProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.inputPage));
    }

    @Test
    public void verifyCompiled()
    {
        BenchmarkData benchmarkData = new BenchmarkData();
        benchmarkData.setup();
        BenchmarkPageProcessor benchmarkPageProcessor = new BenchmarkPageProcessor();
        benchmarkPageProcessor.compiled(benchmarkData);
    }

    @Benchmark
    public List<Optional<Page>> identityProjection(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.identityProjectionProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.inputPage));
    }

    @Test
    public void verifyIdentityProjection()
    {
        BenchmarkData benchmarkData = new BenchmarkData();
        benchmarkData.setup();
        BenchmarkPageProcessor benchmarkPageProcessor = new BenchmarkPageProcessor();
        benchmarkPageProcessor.identityProjection(benchmarkData);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int EXTENDED_PRICE = 0;
        private static final int DISCOUNT = 1;
        private static final int SHIP_DATE = 2;
        private static final int QUANTITY = 3;
        private static final int EXTENDED_PRICE_IN_CENTS = 4;
        private static final int DISCOUNT_PERCENT = 5;
        private static final int POSITION_COUNT = 10_000;

        private static final Slice MIN_SHIP_DATE = utf8Slice("1994-01-01");
        private static final Slice MAX_SHIP_DATE = utf8Slice("1995-01-01");

        private MetadataManager metadataManager = createTestMetadataManager();
        private FunctionAndTypeManager functionManager = metadataManager.getFunctionAndTypeManager();
        private PageProcessor compiledProcessor;
        private Tpch1FilterAndProject handcodedProcessor;
        private PageProcessor identityProjectionProcessor;
        private Page inputPage;

        @SuppressWarnings("unused")
        @Param({"always", "never", "partial"})
        private String filterFails = "partial";

        @SuppressWarnings("unused")
        @Param({"BIGINT", "DOUBLE"})
        private String projectionDataType = "DOUBLE";

        @Setup
        public void setup()
        {
            inputPage = createInputPage();

            compiledProcessor = new ExpressionCompiler(metadataManager, new PageFunctionCompiler(metadataManager, 0))
                    .compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(createFilterExpression(functionManager)), ImmutableList.of(createProjectExpression(functionManager)))
                    .get();
            handcodedProcessor = new Tpch1FilterAndProject();
            identityProjectionProcessor = createIndentityProjectionPageProcessor();
        }

        private static Page createInputPage()
        {
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, DOUBLE, BIGINT, BIGINT));
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            Iterator<LineItem> iterator = lineItemGenerator.iterator();
            for (int i = 0; i < POSITION_COUNT; i++) {
                pageBuilder.declarePosition();

                LineItem lineItem = iterator.next();
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.getExtendedPrice());
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.getDiscount());
                DATE.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), lineItem.getShipDate());
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.getQuantity());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(EXTENDED_PRICE_IN_CENTS), lineItem.getExtendedPriceInCents());
                BIGINT.writeLong(pageBuilder.getBlockBuilder(DISCOUNT_PERCENT), lineItem.getDiscountPercent());
            }
            return pageBuilder.build();
        }

        private final RowExpression createFilterExpression(FunctionAndTypeManager functionAndTypeManager)
        {
            if (filterFails.equals("never")) {
                return new ConstantExpression(true, BOOLEAN);
            }
            else if (filterFails.equals("always")) {
                // where shipdate >= '1994-01-01'
                //    and shipdate < '1995-01-01'
                //    and discount >= 0.05
                //    and discount <= 0.07
                //    and quantity < 24;
                // This filters out 100% of rows.
                return specialForm(
                        AND,
                        BOOLEAN,
                        call(GREATER_THAN_OR_EQUAL.name(),
                                functionManager.resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(VARCHAR, VARCHAR)),
                                BOOLEAN,
                                field(SHIP_DATE, VARCHAR),
                                constant(MIN_SHIP_DATE, VARCHAR)),
                        specialForm(
                                AND,
                                BOOLEAN,
                                call(LESS_THAN.name(),
                                        functionManager.resolveOperator(LESS_THAN, fromTypes(VARCHAR, VARCHAR)),
                                        BOOLEAN,
                                        field(SHIP_DATE, VARCHAR),
                                        constant(MAX_SHIP_DATE, VARCHAR)),
                                specialForm(
                                        AND,
                                        BOOLEAN,
                                        call(GREATER_THAN_OR_EQUAL.name(),
                                                functionManager.resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(DOUBLE, DOUBLE)),
                                                BOOLEAN,
                                                field(DISCOUNT, DOUBLE),
                                                constant(0.05, DOUBLE)),
                                        specialForm(
                                                AND,
                                                BOOLEAN,
                                                call(LESS_THAN_OR_EQUAL.name(),
                                                        functionManager.resolveOperator(LESS_THAN_OR_EQUAL, fromTypes(DOUBLE, DOUBLE)),
                                                        BOOLEAN,
                                                        field(DISCOUNT, DOUBLE),
                                                        constant(0.07, DOUBLE)),
                                                call(LESS_THAN.name(),
                                                        functionManager.resolveOperator(LESS_THAN, fromTypes(DOUBLE, DOUBLE)),
                                                        BOOLEAN,
                                                        field(QUANTITY, DOUBLE),
                                                        constant(24.0, DOUBLE))))));
            }
            else {
                // Filter out 50% rows
                return call(GREATER_THAN_OR_EQUAL.name(),
                        functionManager.resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(DOUBLE, DOUBLE)),
                        BOOLEAN,
                        field(DISCOUNT, DOUBLE),
                        constant(0.05, DOUBLE));
            }
        }

        private final RowExpression createProjectExpression(FunctionAndTypeManager functionAndTypeManager)
        {
            switch (projectionDataType) {
                case "BIGINT":
                    return call(
                            MULTIPLY.name(),
                            functionAndTypeManager.resolveOperator(MULTIPLY, fromTypes(BIGINT, BIGINT)),
                            BIGINT,
                            field(EXTENDED_PRICE_IN_CENTS, BIGINT),
                            field(DISCOUNT_PERCENT, BIGINT));

                case "DOUBLE":
                    return call(
                            MULTIPLY.name(),
                            functionAndTypeManager.resolveOperator(MULTIPLY, fromTypes(DOUBLE, DOUBLE)),
                            DOUBLE,
                            field(EXTENDED_PRICE, DOUBLE),
                            field(DISCOUNT, DOUBLE));

                default:
                    return null;
            }
        }

        private PageProcessor createIndentityProjectionPageProcessor()
        {
            PageFilter filter;
            if (filterFails.equals("always")) {
                filter = new TestPageProcessor.TestingPageFilter(positionsRange(0, 0));
            }
            else if (filterFails.equals("never")) {
                filter = new TestPageProcessor.TestingPageFilter(positionsRange(0, POSITION_COUNT));
            }
            else {
                // Filter out 50% rows
                int[] positions = IntStream.range(0, POSITION_COUNT / 2).map(x -> x * 2).toArray();
                filter = new TestPageProcessor.TestingPageFilter(positionsList(positions, 0, POSITION_COUNT / 2));
            }

            return new PageProcessor(
                    Optional.of(filter),
                    ImmutableList.of(createInputPageProjectionWithOutputs(0, metadataManager.getType(parseTypeSignature(projectionDataType)), 0)),
                    OptionalInt.of(MAX_BATCH_SIZE));
        }

        private PageProjectionWithOutputs createInputPageProjectionWithOutputs(int inputChannel, Type type, int outputChannel)
        {
            return new PageProjectionWithOutputs(new InputPageProjection(inputChannel), new int[] {outputChannel});
        }

        private final class Tpch1FilterAndProject
        {
            public int process(Page page, int start, int end, PageBuilder pageBuilder)
            {
                Block discountBlock = page.getBlock(DISCOUNT);
                int position = start;
                for (; position < end; position++) {
                    // where shipdate >= '1994-01-01'
                    //    and shipdate < '1995-01-01'
                    //    and discount >= 0.05
                    //    and discount <= 0.07
                    //    and quantity < 24;
                    if (filter(position, discountBlock, page.getBlock(SHIP_DATE), page.getBlock(QUANTITY))) {
                        project(position, pageBuilder, page.getBlock(EXTENDED_PRICE), discountBlock);
                    }
                }

                return position;
            }

            private void project(int position, PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock)
            {
                pageBuilder.declarePosition();
                if (discountBlock.isNull(position) || extendedPriceBlock.isNull(position)) {
                    pageBuilder.getBlockBuilder(0).appendNull();
                }
                else {
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), DOUBLE.getDouble(extendedPriceBlock, position) * DOUBLE.getDouble(discountBlock, position));
                }
            }

            private boolean filter(int position, Block discountBlock, Block shipDateBlock, Block quantityBlock)
            {
                if (filterFails.equals("never")) {
                    return true;
                }
                else if (filterFails.equals("always")) {
                    return !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MIN_SHIP_DATE) >= 0 &&
                            !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MAX_SHIP_DATE) < 0 &&
                            !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05 &&
                            !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) <= 0.07 &&
                            !quantityBlock.isNull(position) && DOUBLE.getDouble(quantityBlock, position) < 24;
                }
                else {
                    // Filter out 50% rows
                    return !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05;
                }
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkPageProcessor.class.getSimpleName() + ".*")
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(options).run();
    }
}
