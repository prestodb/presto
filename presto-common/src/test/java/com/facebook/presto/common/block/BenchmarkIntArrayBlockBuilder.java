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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.Assert;

import java.util.Random;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(value = 2, jvmArgs = {"-Xmx16g"})
@Warmup(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkIntArrayBlockBuilder
{
    int BLOCKS_CNT = 20_000;
    int TOTAL_ROWS = 1000;
    Type type = INTEGER;

    private Block[] inMapBlocks;
    private Block[] valueBlocks;

    @Setup
    public void setup()
    {
        Random rnd = new Random(1);

        BlockBuilder[] inMapBlockBuilders = new BlockBuilder[BLOCKS_CNT];
        BlockBuilder[] valueBlockBuilders = new BlockBuilder[BLOCKS_CNT];

        for (int i = 0; i < BLOCKS_CNT; i++) {
            inMapBlockBuilders[i] = BOOLEAN.createBlockBuilder(null, 1);
            valueBlockBuilders[i] = INTEGER.createBlockBuilder(null, 1);
        }

        int value = 0;
        for (int i = 0; i < TOTAL_ROWS; i++) {
            for (int j = 0; j < BLOCKS_CNT; j++) {
                boolean isInMap = rnd.nextBoolean();
                BOOLEAN.writeBoolean(inMapBlockBuilders[j], isInMap);
                if (isInMap) {
                    INTEGER.writeLong(valueBlockBuilders[j], value++);
                }
            }
        }

        this.inMapBlocks = new Block[BLOCKS_CNT];
        this.valueBlocks = new Block[BLOCKS_CNT];
        for (int i = 0; i < BLOCKS_CNT; i++) {
            inMapBlocks[i] = inMapBlockBuilders[i].build();
            valueBlocks[i] = valueBlockBuilders[i].build();
        }
    }

    @Benchmark
    public Block benchmarkNewBatchFlat()
    {
        int keyCount = inMapBlocks.length;
        int[] valueBlockPositions = new int[keyCount];

        IntArrayBatchPositions batchPositions = new IntArrayBatchPositions();

        int destPosition = 0;
        int rowCount = TOTAL_ROWS;
        int maxBatchSize = 32;

        int inMapIndex = 0;
        while (rowCount > 0) {
            int batchSize = Math.min(rowCount, maxBatchSize);

            for (int row = 0; row < batchSize; row++) {
                for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                    if (BOOLEAN.getBoolean(inMapBlocks[keyIndex], inMapIndex)) {
                        batchPositions.capture(valueBlocks[keyIndex], valueBlockPositions[keyIndex], destPosition);
                        valueBlockPositions[keyIndex]++;
                        destPosition++;
                    }
                }
                inMapIndex++;
            }

            batchPositions.sink();
            rowCount -= batchSize;
        }

        return batchPositions.buildBlock();
    }

    @Benchmark
    public Block benchmarkOldFlat()
    {
        BlockBuilder bb = type.createBlockBuilder(null, 1000);

        int keyCount = inMapBlocks.length;
        int[] valueBlockPositions = new int[keyCount];
        int rowCount = TOTAL_ROWS;

        for (int row = 0; row < rowCount; row++) {
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                if (BOOLEAN.getBoolean(inMapBlocks[keyIndex], row)) {
                    type.appendTo(valueBlocks[keyIndex], valueBlockPositions[keyIndex], bb);
                    valueBlockPositions[keyIndex]++;
                }
            }
        }

        return bb.build();
    }

    public static void main(String[] args)
            throws Throwable
    {
        b(args);
    }

    public static void r(String[] args)
            throws Throwable
    {
        BenchmarkIntArrayBlockBuilder b = new BenchmarkIntArrayBlockBuilder();
        b.setup();
        Block oldBlock = b.benchmarkOldFlat();
        Block newBlock = b.benchmarkNewBatchFlat();
        System.out.println("Position Count: " + oldBlock.getPositionCount());

        Assert.assertEquals(oldBlock.getPositionCount(), newBlock.getPositionCount());
        for (int i = 0; i < oldBlock.getPositionCount(); i++) {
            Assert.assertEquals(oldBlock.isNull(i), newBlock.isNull(i));
            if (!oldBlock.isNull(i)) {
                Assert.assertEquals(oldBlock.getInt(i), newBlock.getInt(i));
            }
        }
    }

    public static void b(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkIntArrayBlockBuilder.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
