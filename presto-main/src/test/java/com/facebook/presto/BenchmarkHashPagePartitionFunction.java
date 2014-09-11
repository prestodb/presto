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
package com.facebook.presto;

import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkHashPagePartitionFunction
{
    private static final List<Type> types = ImmutableList.<Type>of(BigintType.BIGINT, BigintType.BIGINT, BigintType.BIGINT);

    @Benchmark
    public List<Page> benchmarkHashPagePartitionFunction(Data data)
    {
        PagePartitionFunction hashPagePartitionFunction = new HashPagePartitionFunction(0, 8, ImmutableList.of(0, 1), 2, types);
        return hashPagePartitionFunction.partition(data.pages);
    }

    @State(Scope.Thread)
    public static class Data
    {
        public List<Page> pages;

        @Setup(Level.Iteration)
        public void init()
        {
            pages = new ArrayList<>();
            PageBuilder pageBuilder = new PageBuilder(types);
            pageBuilder.reset();

            // Create 3 pages of ~64KB
            for (int numPage = 0; numPage < 3; numPage++) {
                pageBuilder.reset();
                for (int numRow = 0; numRow < 3000; numRow++) {
                    for (int numChannel = 0; numChannel < types.size(); numChannel++) {
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(numChannel);
                        Type type = types.get(numChannel);
                        type.writeLong(blockBuilder, ThreadLocalRandom.current().nextLong());
                    }
                }
                pages.add(pageBuilder.build());
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashPagePartitionFunction.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
