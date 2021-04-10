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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.MapType;
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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapConcat
{
    private static final int POSITIONS = 1000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Optional<Page>> mapConcat(BenchmarkData data)
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
        private String name = "map_concat";

        @Param({"left_empty", "right_empty", "both_empty", "non_empty"})
        private String mapConfig = "non_empty";

        @Param({"10", "100", "1000"})
        private int keyCount = 100;

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));

            List<String> keyList1 = createRandomStringListFromSet(keyCount);
            List<String> keyList2 = createRandomStringListFromSet(keyCount);

            List<String> leftKeys;
            List<String> rightKeys;
            switch (mapConfig) {
                case "left_empty":
                    leftKeys = ImmutableList.of();
                    rightKeys = keyList1;
                    break;
                case "right_empty":
                    leftKeys = keyList1;
                    rightKeys = ImmutableList.of();
                    break;
                case "both_empty":
                    leftKeys = ImmutableList.of();
                    rightKeys = ImmutableList.of();
                    break;
                case "non_empty":
                    leftKeys = keyList1;
                    rightKeys = keyList2;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            MapType mapType = mapType(createUnboundedVarcharType(), DOUBLE);

            Block leftKeyBlock = createKeyBlock(POSITIONS, leftKeys);
            Block leftValueBlock = createValueBlock(POSITIONS, leftKeys.size());
            Block leftBlock = createMapBlock(mapType, POSITIONS, leftKeyBlock, leftValueBlock);

            Block rightKeyBlock = createKeyBlock(POSITIONS, rightKeys);
            Block rightValueBlock = createValueBlock(POSITIONS, rightKeys.size());
            Block rightBlock = createMapBlock(mapType, POSITIONS, rightKeyBlock, rightValueBlock);

            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();

            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(name, fromTypes(mapType, mapType));
            projectionsBuilder.add(new CallExpression(
                    name,
                    functionHandle,
                    mapType,
                    ImmutableList.of(field(0, mapType), field(1, mapType))));

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
            page = new Page(leftBlock, rightBlock);
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }

        private static Block createMapBlock(MapType mapType, int positionCount, Block keyBlock, Block valueBlock)
        {
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(positionCount, Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createKeyBlock(int positionCount, List<String> keys)
        {
            Block keyDictionaryBlock = createSliceArrayBlock(keys);
            int[] keyIds = new int[positionCount * keys.size()];
            for (int i = 0; i < keyIds.length; i++) {
                keyIds[i] = i % keys.size();
            }
            return new DictionaryBlock(keyDictionaryBlock, keyIds);
        }

        private static Block createValueBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = DOUBLE.createBlockBuilder(null, positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                DOUBLE.writeDouble(valueBlockBuilder, ThreadLocalRandom.current().nextDouble());
            }
            return valueBlockBuilder.build();
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

        private static List<String> createRandomStringListFromSet(int keyCount)
        {
            Random random = new Random(0);
            List<String> keyList = new ArrayList<>();
            for (int i = 0; i < keyCount; i++) {
                keyList.add(Integer.toString(random.nextInt(keyCount)));
            }
            return keyList;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkMapConcat().mapConcat(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkMapConcat.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
