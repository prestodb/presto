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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingEnvironment.TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(5)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDictionaryBlockGetSizeInBytes
{
    @Benchmark
    public long getSizeInBytes(BenchmarkData data)
    {
        return data.getDictionaryBlock().getSizeInBytes();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS = 100_000;
        @Param({"100", "1000", "10000", "100000"})
        private String selectedPositions = "100";

        private DictionaryBlock dictionaryBlock;

        @Setup(Level.Invocation)
        public void setup()
        {
            dictionaryBlock = new DictionaryBlock(createMapBlock(POSITIONS), generateIds(Integer.parseInt(selectedPositions), POSITIONS));
        }

        private static Block createMapBlock(int positionCount)
        {
            Type keyType = VARCHAR;
            Type valueType = VARCHAR;
            MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
            MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
            MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
            MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
            MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
            MapType mapType = new MapType(
                    keyType,
                    valueType,
                    keyBlockNativeEquals,
                    keyBlockEquals,
                    keyNativeHashCode,
                    keyBlockHashCode);
            Block keyBlock = createDictionaryBlock(generateList("key", positionCount));
            Block valueBlock = createDictionaryBlock(generateList("value", positionCount));
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createDictionaryBlock(List<String> values)
        {
            Block dictionary = createSliceArrayBlock(values);
            int[] ids = new int[values.size()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = i;
            }
            return new DictionaryBlock(dictionary, ids);
        }

        private static Block createSliceArrayBlock(List<String> values)
        {
            // last position is reserved for null
            Slice[] sliceArray = new Slice[values.size() + 1];
            for (int i = 0; i < values.size(); i++) {
                sliceArray[i] = utf8Slice(values.get(i));
            }
            return createSlicesBlock(sliceArray);
        }

        private static List<String> generateList(String prefix, int count)
        {
            ImmutableList.Builder<String> list = ImmutableList.builder();
            for (int i = 0; i < count; i++) {
                list.add(prefix + Integer.toString(i));
            }
            return list.build();
        }

        private static int[] generateIds(int count, int range)
        {
            return new Random().ints(count, 0, range).toArray();
        }

        public DictionaryBlock getDictionaryBlock()
        {
            return dictionaryBlock;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkDictionaryBlockGetSizeInBytes().getSizeInBytes(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDictionaryBlockGetSizeInBytes.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
