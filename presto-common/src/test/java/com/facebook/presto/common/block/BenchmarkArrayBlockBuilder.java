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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(value = 2, jvmArgs = {"-Xmx16g"})
@Warmup(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayBlockBuilder
{
    ArrayType arrayType = new ArrayType(INTEGER);
    List<Block> flatBlocks = new ArrayList<>();
    List<Block> dictionaryBlocks = new ArrayList<>();

    @Setup
    public void setup()
    {
        Random rnd = new Random(123);
        int INPUT_BLOCKS_CNT = 12_000;

        int ENTRIES_COUNT = 1000;
        int ENTRY_SIZE = 50;
        int DICTIONARY_SIZE = 50;

        for (int blockNum = 0; blockNum < INPUT_BLOCKS_CNT; blockNum++) {
            BlockBuilder dictionaryValuesBlockBuilder = arrayType.createBlockBuilder(null, 100);
            for (int i = 0; i < DICTIONARY_SIZE; i++) {
                BlockBuilder elementBuilder = dictionaryValuesBlockBuilder.beginBlockEntry();
                for (int j = 0; j < ENTRY_SIZE; j++) {
                    INTEGER.writeLong(elementBuilder, rnd.nextInt());
                    if (j % 11 == 0) {
                        elementBuilder.appendNull();
                    }
                }
                dictionaryValuesBlockBuilder.closeEntry();
            }

            Block dictValues = dictionaryValuesBlockBuilder.build();
            int[] dictIds = new int[ENTRIES_COUNT];
            for (int i = 0; i < dictIds.length; i++) {
                dictIds[i] = rnd.nextInt(dictValues.getPositionCount());
            }
            DictionaryBlock dictionaryBlock = new DictionaryBlock(ENTRIES_COUNT, dictValues, dictIds);
            dictionaryBlocks.add(dictionaryBlock);
        }

        for (int blockNum = 0; blockNum < INPUT_BLOCKS_CNT; blockNum++) {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 100);
            for (int i = 0; i < ENTRIES_COUNT; i++) {
                BlockBuilder elementBuilder = blockBuilder.beginBlockEntry();
                for (int j = 0; j < ENTRY_SIZE; j++) {
                    INTEGER.writeLong(elementBuilder, rnd.nextInt());
                }
                blockBuilder.closeEntry();
            }

            flatBlocks.add(blockBuilder.build());
        }
    }

    //    @Benchmark
    public Block benchmarkDictAppendTo0()
    {
        BlockBuilder bb1 = arrayType.createBlockBuilder(null, 1000);
        for (Block block : dictionaryBlocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                arrayType.appendTo(block, position, bb1);
            }
        }
        return bb1.build();
    }

    @Benchmark
    public Block benchmarkFlatAppendTo0()
    {
        BlockBuilder bb1 = arrayType.createBlockBuilder(null, 1000);
        int max = flatBlocks.get(0).getPositionCount();

        for (int position = 0; position < max; position++) {
            for (Block block : flatBlocks) {
                arrayType.appendTo(block, position, bb1);
            }
        }
        return bb1.build();
    }


    public static void main(String[] args)
            throws Throwable
    {
        b(args);
    }

    public static void r(String[] args)
            throws Throwable
    {
        BenchmarkArrayBlockBuilder b = new BenchmarkArrayBlockBuilder();
        b.setup();
        ArrayBlock dictB = (ArrayBlock) b.benchmarkDictAppendTo0();
        ArrayBlock flatB = (ArrayBlock) b.benchmarkFlatAppendTo0();

        System.out.println(dictB.getRawElementBlock().getPositionCount());
        System.out.println(flatB.getRawElementBlock().getPositionCount());
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
