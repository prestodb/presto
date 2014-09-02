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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class GroupByHashBenchmark
{
    @Benchmark
    public void testExistingGroupIds(Data data)
    {
        data.groupByHash.getGroupIds(data.pages.get(0));
    }

    @Benchmark
    public void testNewGroupIds(Data data)
    {
        GroupByHash groupByHash = new GroupByHash(ImmutableList.of(BIGINT, BIGINT), new int[] { 0, 1 }, 2, 100);
        groupByHash.getGroupIds(data.pages.get(0));
    }

    @Benchmark
    public void testMixGroupIds(Data data)
    {
        data.groupByHash.getGroupIds(data.pages.get(1));
    }

    @State(Scope.Thread)
    public static class Data
    {
        List<Page> pages;
        GroupByHash groupByHash = new GroupByHash(ImmutableList.of(BIGINT, BIGINT), new int[] { 0, 1 }, 2, 100);
        Block[] containsBlocks = new Block[2];
        Block containsHashBlock;

        @Setup(Level.Iteration)
        public void init()
        {
            pages = new ArrayList<>();
            Block[] blocks = new Block[3];
            int start = 0;
            for (int i = 0; i < 3; i++) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(start, start + 100);
                start += 100;
            }
            pages.add(new Page(blocks));
            groupByHash.getGroupIds(pages.get(0));

            start = 50;
            for (int i = 0; i < 3; i++) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(start, start + 100);
                start = start + 100;
            }
            pages.add(new Page(blocks));

            start = 50;
            for (int i = 0; i < 2; i++) {
                containsBlocks[i] = BlockAssertions.createLongSequenceBlock(start, start + 100);
                start = start + 100;
            }
            containsHashBlock = BlockAssertions.createLongSequenceBlock(start, start + 100);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + GroupByHashBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
