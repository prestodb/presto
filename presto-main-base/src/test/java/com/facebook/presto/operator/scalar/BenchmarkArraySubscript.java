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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArraySubscript
{
    private static final int POSITIONS = 1024;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Optional<Page>> arraySubscript(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"fix-width", "var-width", "dictionary", "array"})
        private String name = "dictionary";

        @Param({"1", "13"})
        private int arraySize = 13;

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));

            ArrayType arrayType;
            Block elementsBlock;
            switch (name) {
                case "fix-width":
                    arrayType = new ArrayType(DOUBLE);
                    elementsBlock = createFixWidthValueBlock(POSITIONS, arraySize);
                    break;
                case "var-width":
                    arrayType = new ArrayType(createUnboundedVarcharType());
                    elementsBlock = createVarWidthValueBlock(POSITIONS, arraySize);
                    break;
                case "dictionary":
                    arrayType = new ArrayType(createUnboundedVarcharType());
                    elementsBlock = createDictionaryValueBlock(POSITIONS, arraySize);
                    break;
                case "array":
                    arrayType = new ArrayType(new ArrayType(createUnboundedVarcharType()));
                    elementsBlock = createArrayBlock(POSITIONS * arraySize, createVarWidthValueBlock(POSITIONS, arraySize));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            Block block = createArrayBlock(POSITIONS, elementsBlock);

            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();

            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveOperator(SUBSCRIPT, fromTypes(arrayType, BIGINT));
            for (int i = 0; i < arraySize; i++) {
                projectionsBuilder.add(new CallExpression(
                        SUBSCRIPT.name(),
                        functionHandle,
                        arrayType.getElementType(),
                        ImmutableList.of(field(0, arrayType), constant((long) i + 1, BIGINT))));
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
            page = new Page(block);
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }

        private static Block createArrayBlock(int positionCount, Block elementsBlock)
        {
            int[] offsets = new int[positionCount + 1];
            int arraySize = elementsBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = arraySize * i;
            }
            return ArrayBlock.fromElementBlock(positionCount, Optional.empty(), offsets, elementsBlock);
        }

        private static Block createFixWidthValueBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = DOUBLE.createBlockBuilder(null, positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                DOUBLE.writeDouble(valueBlockBuilder, ThreadLocalRandom.current().nextDouble());
            }
            return valueBlockBuilder.build();
        }

        private static Block createVarWidthValueBlock(int positionCount, int mapSize)
        {
            Type valueType = createUnboundedVarcharType();
            BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                int wordLength = ThreadLocalRandom.current().nextInt(5, 10);
                valueType.writeSlice(valueBlockBuilder, utf8Slice(randomString(wordLength)));
            }
            return valueBlockBuilder.build();
        }

        private static Block createDictionaryValueBlock(int positionCount, int mapSize)
        {
            double distinctRatio = 0.82;

            int dictionarySize = (int) (positionCount * mapSize * distinctRatio);
            List<String> dictionaryStrings = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                int wordLength = ThreadLocalRandom.current().nextInt(5, 10);
                dictionaryStrings.add(randomString(wordLength));
            }
            Block dictionaryBlock = createSliceArrayBlock(dictionaryStrings);

            int[] keyIds = new int[positionCount * mapSize];
            for (int i = 0; i < keyIds.length; i++) {
                keyIds[i] = ThreadLocalRandom.current().nextInt(0, dictionarySize);
            }
            return new DictionaryBlock(dictionaryBlock, keyIds);
        }

        private static String randomString(int length)
        {
            String symbols = "abcdefghijklmnopqrstuvwxyz";
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = symbols.charAt(ThreadLocalRandom.current().nextInt(symbols.length()));
            }
            return new String(chars);
        }

        private static Block createSliceArrayBlock(List<String> keys)
        {
            // last position is reserved for null
            Slice[] sliceArray = new Slice[keys.size() + 1];
            for (int i = 0; i < keys.size(); i++) {
                sliceArray[i] = utf8Slice(keys.get(i));
            }
            return createSlicesBlock(sliceArray);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraySubscript().arraySubscript(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkArraySubscript.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
