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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.MapType;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
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
    private static final int POSITIONS = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object mapConcat(BenchmarkData data)
            throws Throwable
    {
        int position = 0;
        List<Page> pages = new ArrayList<>();
        while (position < data.getPage().getPositionCount()) {
            position = data.getPageProcessor().process(SESSION, data.getPage(), position, data.getPage().getPositionCount(), data.getPageBuilder());
            pages.add(data.getPageBuilder().build());
            data.getPageBuilder().reset();
        }
        return pages;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private String name = "map_concat";

        @Param({"left_empty", "right_empty", "both_empty", "non_empty"})
        private String mapConfig = "left_empty";

        private PageBuilder pageBuilder;
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata);

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

            MapType mapType = new MapType(createUnboundedVarcharType(), DOUBLE);

            Block leftKeyBlock = createKeyBlock(POSITIONS, leftKeys);
            Block leftValueBlock = createValueBlock(POSITIONS, leftKeys.size());
            Block leftBlock = createMapBlock(POSITIONS, leftKeyBlock, leftValueBlock);

            Block rightKeyBlock = createKeyBlock(POSITIONS, rightKeys);
            Block rightValueBlock = createValueBlock(POSITIONS, rightKeys.size());
            Block rightBlock = createMapBlock(POSITIONS, rightKeyBlock, rightValueBlock);

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
                    ImmutableList.of(new InputReferenceExpression(0, mapType), new InputReferenceExpression(1, mapType))));

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(new ConstantExpression(true, BooleanType.BOOLEAN), projections).get();
            pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(Collectors.toList()));
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

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }

        private static Block createMapBlock(int positionCount, Block keyBlock, Block valueBlock)
        {
            InterleavedBlock interleavedBlock = new InterleavedBlock(new Block[]{keyBlock, valueBlock});
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * 2 * i;
            }
            return new ArrayBlock(positionCount, new boolean[positionCount], offsets, interleavedBlock);
        }

        private static Block createKeyBlock(int positionCount, List<String> keys)
        {
            Block keyDictionaryBlock = createSliceArrayBlock(keys);
            int[] keyIds = new int[positionCount * keys.size()];
            for (int i = 0; i < keyIds.length; i++) {
                keyIds[i] = i % keys.size();
            }
            return new DictionaryBlock(positionCount * keys.size(), keyDictionaryBlock, keyIds);
        }

        private static Block createValueBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), positionCount * mapSize);
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
            return new SliceArrayBlock(sliceArray.length, sliceArray);
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
