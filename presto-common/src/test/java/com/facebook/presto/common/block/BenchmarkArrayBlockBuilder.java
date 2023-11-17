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

import com.facebook.presto.common.type.ArrayType;
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

import java.util.Random;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(value = 1, jvmArgs = {"-Xms8g","-Xmx8g"})
@Warmup(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayBlockBuilder
{
    // number of lists in a block
    private static final int ENTRIES_COUNT = 500_000;

    // size of a single list
    private static final int ENTRY_SIZE = 500;
    private static final int DICTIONARY_SIZE = 50;
    private static final int SEED = 2023;

    private static final ArrayType ARRAY_TYPE = new ArrayType(INTEGER);
    private Block flatBlock;
    private Block dictionaryBlock;

    @Param({
            "1",
            "10",
            "100",
            "500"
    })
    public int listSize;

//    @Param({"0", "1"})
    public int mode;

    @Param({"false", "true"})
    public boolean hasNulls;
    private ArrayBlockBuilder blockBuilder;

    @Setup
    public void setup()
    {
        Random rnd = new Random(SEED);
//
//        // build a dictionary block
//        BlockBuilder dictionaryBlockBuilder = ARRAY_TYPE.createBlockBuilder(null, 100);
//        for (int i = 0; i < DICTIONARY_SIZE; i++) {
//            BlockBuilder elementBuilder = dictionaryBlockBuilder.beginBlockEntry();
//            for (int j = 0; j < listSize; j++) {
//                INTEGER.writeLong(elementBuilder, rnd.nextInt());
//            }
//            dictionaryBlockBuilder.closeEntry();
//        }
//
//        Block dictValues = dictionaryBlockBuilder.build();
//        int[] dictIds = new int[ENTRIES_COUNT];
//        for (int i = 0; i < dictIds.length; i++) {
//            dictIds[i] = rnd.nextInt(dictValues.getPositionCount());
//        }
//        dictionaryBlock = new DictionaryBlock(ENTRIES_COUNT, dictValues, dictIds);

        // build a flat block
        long cnt = 0;
        BlockBuilder flatBlockBuilder = ARRAY_TYPE.createBlockBuilder(null, ENTRIES_COUNT);
        for (int i = 0; i < ENTRIES_COUNT; i++) {
            BlockBuilder elementBuilder = flatBlockBuilder.beginBlockEntry();
            for (int j = 0; j < listSize; j++) {
                if (hasNulls && cnt++ % 10 == 0) {
                    elementBuilder.appendNull();
                }
                else {
                    INTEGER.writeLong(elementBuilder, rnd.nextInt());
                }
            }
            flatBlockBuilder.closeEntry();
        }
        flatBlock = flatBlockBuilder.build();

    }

    @Setup(Level.Iteration)
    public void setupBB()
    {
        this.blockBuilder = (ArrayBlockBuilder) ARRAY_TYPE.createBlockBuilder(null, flatBlock.getPositionCount());
    }

    //    @Benchmark
    public Block benchmarkDictAppendTo()
    {
        Block block = dictionaryBlock;
        BlockBuilder blockBuilder = ARRAY_TYPE.createBlockBuilder(null, 1000);
        for (int position = 0; position < block.getPositionCount(); position++) {
            ARRAY_TYPE.appendTo(block, position, blockBuilder);
        }
        return blockBuilder.build();
    }

    @Benchmark
    public Block benchmarkFlatAppendToOriginal()
    {
        Block block = flatBlock;
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.appendStructureInternal(block, i);
        }
        return blockBuilder.build();
    }

    @Benchmark
    public Block benchmarkFlatAppendToNew1()
    {
        Block block = flatBlock;
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.appendStructureInternalNew(block, i);
        }
        return blockBuilder.build();
    }


    public static void main(String[] args)
            throws Throwable
    {
        b(args);
    }

    public static void r(String[] args)
            throws Throwable
    {
//        BenchmarkArrayBlockBuilder b = new BenchmarkArrayBlockBuilder();
//        b.setup();
//        ArrayBlock dictB = (ArrayBlock) b.benchmarkDictAppendTo();
//        ArrayBlock flatB = (ArrayBlock) b.benchmarkFlatAppendTo();
//
//        System.out.println(dictB.getRawElementBlock().getPositionCount());
//        System.out.println(flatB.getRawElementBlock().getPositionCount());
    }

    public static void b(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayBlockBuilder.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}