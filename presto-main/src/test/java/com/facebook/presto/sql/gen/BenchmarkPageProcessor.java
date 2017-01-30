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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(5)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkPageProcessor
{
    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    private static final Slice MIN_SHIP_DATE = utf8Slice("1994-01-01");
    private static final Slice MAX_SHIP_DATE = utf8Slice("1995-01-01");

    private Page inputPage;
    private PageProcessor handCodedProcessor;
    private PageProcessor compiledProcessor;

    @Setup
    public void setup()
    {
        inputPage = createInputPage();

        handCodedProcessor = new Tpch1FilterAndProject();

        compiledProcessor = new ExpressionCompiler(MetadataManager.createTestMetadataManager()).compilePageProcessor(FILTER, ImmutableList.of(PROJECT)).get();
    }

    @Benchmark
    public Page handCoded()
    {
        return execute(inputPage, handCodedProcessor);
    }

    @Benchmark
    public Page compiled()
    {
        return execute(inputPage, compiledProcessor);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        new BenchmarkPageProcessor().setup();

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkPageProcessor.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    public static Page execute(Page inputPage, PageProcessor processor)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE));
        int count = processor.process(null, inputPage, 0, inputPage.getPositionCount(), pageBuilder);
        checkState(count == inputPage.getPositionCount());
        return pageBuilder.build();
    }

    public static Page createInputPage()
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, DOUBLE));
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < 10_000; i++) {
            pageBuilder.declarePosition();

            LineItem lineItem = iterator.next();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.getExtendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.getDiscount());
            DATE.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), lineItem.getShipDate());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.getQuantity());
        }
        return pageBuilder.build();
    }

    private static final class Tpch1FilterAndProject
            implements PageProcessor
    {
        @Override
        public int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder)
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

        @Override
        public Page processColumnar(ConnectorSession session, Page page, List<? extends Type> types)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page processColumnarDictionary(ConnectorSession session, Page page, List<? extends Type> types)
        {
            throw new UnsupportedOperationException();
        }

        private static void project(int position, PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock)
        {
            pageBuilder.declarePosition();
            if (discountBlock.isNull(position) || extendedPriceBlock.isNull(position)) {
                pageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), DOUBLE.getDouble(extendedPriceBlock, position) * DOUBLE.getDouble(discountBlock, position));
            }
        }

        private static boolean filter(int position, Block discountBlock, Block shipDateBlock, Block quantityBlock)
        {
            return !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MIN_SHIP_DATE) >= 0 &&
                    !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MAX_SHIP_DATE) < 0 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) <= 0.07 &&
                    !quantityBlock.isNull(position) && DOUBLE.getDouble(quantityBlock, position) < 24;
        }
    }

    // where shipdate >= '1994-01-01'
    //    and shipdate < '1995-01-01'
    //    and discount >= 0.05
    //    and discount <= 0.07
    //    and quantity < 24;
    private static final RowExpression FILTER = call(new Signature("AND", SCALAR, parseTypeSignature(StandardTypes.BOOLEAN)),
            BOOLEAN,
            call(internalOperator(OperatorType.GREATER_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                    BOOLEAN,
                    field(SHIP_DATE, VARCHAR),
                    constant(MIN_SHIP_DATE, VARCHAR)),
            call(new Signature("AND", SCALAR, parseTypeSignature(StandardTypes.BOOLEAN)),
                    BOOLEAN,
                    call(internalOperator(OperatorType.LESS_THAN, BOOLEAN.getTypeSignature(), VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                            BOOLEAN,
                            field(SHIP_DATE, VARCHAR),
                            constant(MAX_SHIP_DATE, VARCHAR)),
                    call(new Signature("AND", SCALAR, parseTypeSignature(StandardTypes.BOOLEAN)),
                            BOOLEAN,
                            call(internalOperator(OperatorType.GREATER_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()),
                                    BOOLEAN,
                                    field(DISCOUNT, DOUBLE),
                                    constant(0.05, DOUBLE)),
                            call(new Signature("AND", SCALAR, parseTypeSignature(StandardTypes.BOOLEAN)),
                                    BOOLEAN,
                                    call(internalOperator(OperatorType.LESS_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()),
                                            BOOLEAN,
                                            field(DISCOUNT, DOUBLE),
                                            constant(0.07, DOUBLE)),
                                    call(internalOperator(OperatorType.LESS_THAN, BOOLEAN.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()),
                                            BOOLEAN,
                                            field(QUANTITY, DOUBLE),
                                            constant(24.0, DOUBLE))))));

    private static final RowExpression PROJECT = call(
            internalOperator(OperatorType.MULTIPLY, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()),
            DOUBLE,
            field(EXTENDED_PRICE, DOUBLE),
            field(DISCOUNT, DOUBLE));
}
