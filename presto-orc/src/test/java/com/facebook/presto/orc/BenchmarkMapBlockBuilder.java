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
package com.facebook.presto.orc;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 2, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapBlockBuilder
{
    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkMapBlockBuilder.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Benchmark
    public void blockBuilder(BenchmarkData data)
    {
        MapBlockBuilder mapBlockBuilder = data.getMapBlockBuilder();
        int positionCount = data.getPositionCount();
        int outerElementCount = data.getOuterElementCount();
        int innerElementCount = data.getInnerElementCount();

        for (int position = 0; position < positionCount; position++) {
            BlockBuilder outerBlockBuilder = mapBlockBuilder.beginBlockEntry();
            for (int outer = 0; outer < outerElementCount; outer++) {
                BIGINT.writeLong(outerBlockBuilder, outer);
                BlockBuilder innerBlockBuilder = outerBlockBuilder.beginBlockEntry();
                for (int inner = 0; inner < innerElementCount; inner++) {
                    BIGINT.writeLong(innerBlockBuilder, inner * 2L); // inner key
                    BIGINT.writeLong(innerBlockBuilder, inner * 2L + 1); // inner key
                }
                outerBlockBuilder.closeEntry();
            }
            mapBlockBuilder.closeEntry();
        }
    }

    @Benchmark
    public void directBuilder(BenchmarkData data)
    {
        MapBlockBuilder mapBlockBuilder = data.getMapBlockBuilder();
        int positionCount = data.getPositionCount();
        int outerElementCount = data.getOuterElementCount();
        int innerElementCount = data.getInnerElementCount();

        for (int position = 0; position < positionCount; position++) {
            mapBlockBuilder.beginDirectEntry();
            BlockBuilder outerKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
            for (int outer = 0; outer < outerElementCount; outer++) {
                BIGINT.writeLong(outerKeyBuilder, outer);
            }

            MapBlockBuilder outerValueBuilder = (MapBlockBuilder) mapBlockBuilder.getValueBlockBuilder();
            for (int outer = 0; outer < outerElementCount; outer++) {
                outerValueBuilder.beginDirectEntry();
                BlockBuilder innerKeyBuilder = outerValueBuilder.getKeyBlockBuilder();
                for (int inner = 0; inner < innerElementCount; inner++) {
                    BIGINT.writeLong(innerKeyBuilder, inner * 2L);
                }

                BlockBuilder innerValueBuilder = outerValueBuilder.getValueBlockBuilder();
                for (int inner = 0; inner < innerElementCount; inner++) {
                    BIGINT.writeLong(innerValueBuilder, inner * 2L + 1);
                }

                outerValueBuilder.closeEntry();
            }
            mapBlockBuilder.closeEntry();
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final MethodHandle KEY_NATIVE_EQUALS = getOperatorMethodHandle(OperatorType.EQUAL, BIGINT, BIGINT);
        private static final MethodHandle KEY_BLOCK_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT), nativeValueGetter(BIGINT));
        private static final MethodHandle KEY_NATIVE_HASH_CODE = getOperatorMethodHandle(OperatorType.HASH_CODE, BIGINT);
        private static final MethodHandle KEY_BLOCK_HASH_CODE = compose(KEY_NATIVE_HASH_CODE, nativeValueGetter(BIGINT));

        @Param("100")
        private String positionCount = "100";

        @Param({
                "10",
                "100"
        })
        private String numberOfOuterElements = "100";

        @Param({
                "10",
                "100"
        })
        private String numberOfInnerElements = "100";

        int getPositionCount()
        {
            return Integer.parseInt(positionCount);
        }

        int getOuterElementCount()
        {
            return Integer.parseInt(numberOfOuterElements);
        }

        int getInnerElementCount()
        {
            return Integer.parseInt(numberOfInnerElements);
        }

        MapBlockBuilder getMapBlockBuilder()
        {
            MapType innerMapType = new MapType(
                    BIGINT,
                    BIGINT,
                    KEY_BLOCK_EQUALS,
                    KEY_BLOCK_HASH_CODE);

            MapType mapType = new MapType(
                    BIGINT,
                    innerMapType,
                    KEY_BLOCK_EQUALS,
                    KEY_BLOCK_HASH_CODE);

            return (MapBlockBuilder) mapType.createBlockBuilder(null, 1024);
        }
    }
}
