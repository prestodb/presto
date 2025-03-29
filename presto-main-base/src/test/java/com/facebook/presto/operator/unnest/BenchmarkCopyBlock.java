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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.Metadata;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createRandomBlockForType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCopyBlock
{
    private static final int POSITIONS_PER_PAGE = 10000;
    private static final int BLOCK_COUNT = 100;

    @Benchmark
    public void copyBlockByLoop(BenchmarkData data)
    {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = data.blocks.get(i);
            int positionCount = block.getPositionCount();

            long[] values = new long[positionCount];
            for (int j = 0; j < positionCount; j++) {
                values[j] = block.getLong(j);
            }

            boolean[] valueIsNull = copyIsNulls(block);

            Block outputBlock = new LongArrayBlock(positionCount, Optional.of(valueIsNull), values);
        }
    }

    @Benchmark
    public void copyBlockByAppend(BenchmarkData data)
    {
        LongArrayBlockBuilder longArrayBlockBuilder = new LongArrayBlockBuilder(null, POSITIONS_PER_PAGE);
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = data.blocks.get(i);
            int positionCount = block.getPositionCount();

            for (int j = 0; j < positionCount; j++) {
                BIGINT.appendTo(block, j, longArrayBlockBuilder);
            }

            Block outputBlock = longArrayBlockBuilder.build();
        }
    }

    @Benchmark
    public void copyBlockByWriteLong(BenchmarkData data)
    {
        LongArrayBlockBuilder longArrayBlockBuilder = new LongArrayBlockBuilder(null, POSITIONS_PER_PAGE);
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = data.blocks.get(i);
            int positionCount = block.getPositionCount();

            for (int j = 0; j < positionCount; j++) {
                longArrayBlockBuilder.writeLong(block.getLong(i));
            }

            Block outputBlock = longArrayBlockBuilder.build();
        }
    }

    private static boolean[] copyIsNulls(Block block)
    {
        int positionCount = block.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount + 1];
        if (block.mayHaveNull()) {
            for (int i = 0; i < positionCount; i++) {
                valueIsNull[i] = block.isNull(i);
            }
        }
        return valueIsNull;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private String typeSignature = "bigint";

        @SuppressWarnings("unused")
        @Param({"0.0", "0.2"})
        private float primitiveNullsRatio;

        List<Block> blocks = new ArrayList<>();

        @Setup
        public void setup()
        {
            Metadata metadata = createTestMetadataManager();
            Type type = getType(metadata, typeSignature).get();

            for (int i = 0; i < BLOCK_COUNT; i++) {
                blocks.add(createRandomBlockForType(
                        type,
                        POSITIONS_PER_PAGE,
                        primitiveNullsRatio,
                        0.0f,
                        false,
                        ImmutableList.of()));
            }
        }

        public Optional<Type> getType(Metadata metadata, String typeString)
        {
            if (typeString.equals("NONE")) {
                return Optional.empty();
            }
            TypeSignature signature = TypeSignature.parseTypeSignature(typeString);
            return Optional.of(metadata.getType(signature));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkCopyBlock.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
