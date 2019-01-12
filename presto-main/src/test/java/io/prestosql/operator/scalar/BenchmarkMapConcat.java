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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.type.MapType;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.RowExpression;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.util.StructuralTestUtil.mapType;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapConcat
{
    private static final int POSITIONS = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Optional<Page>> mapConcat(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
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
        private String mapConfig = "left_empty";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));

            List<String> leftKeys;
            List<String> rightKeys;
            switch (mapConfig) {
                case "left_empty":
                    leftKeys = ImmutableList.of();
                    rightKeys = ImmutableList.of("a", "b", "c");
                    break;
                case "right_empty":
                    leftKeys = ImmutableList.of("a", "b", "c");
                    rightKeys = ImmutableList.of();
                    break;
                case "both_empty":
                    leftKeys = ImmutableList.of();
                    rightKeys = ImmutableList.of();
                    break;
                case "non_empty":
                    leftKeys = ImmutableList.of("a", "b", "c");
                    rightKeys = ImmutableList.of("d", "b", "c");
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

            Signature signature = new Signature(
                    name,
                    FunctionKind.SCALAR,
                    mapType.getTypeSignature(),
                    mapType.getTypeSignature(),
                    mapType.getTypeSignature());
            projectionsBuilder.add(new CallExpression(
                    signature,
                    mapType,
                    ImmutableList.of(field(0, mapType), field(1, mapType))));

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
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
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
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
