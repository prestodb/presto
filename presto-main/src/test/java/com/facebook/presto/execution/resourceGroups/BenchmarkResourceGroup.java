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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.MockQueryExecution;
import com.facebook.presto.execution.resourceGroups.ResourceGroup.RootResourceGroup;
import io.airlift.units.DataSize;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkResourceGroup
{
    @Benchmark
    public Object benchmark(BenchmarkData data)
            throws Throwable
    {
        data.getRoot().processQueuedQueries();
        return data.getRoot();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1000", "10000", "100000"})
        private int children = 1000;

        @Param({"100", "1000", "10000"})
        private int queries = 100;

        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private RootResourceGroup root;

        @Setup
        public void setup()
        {
            root = new RootResourceGroup("root", (group, export) -> { }, executor);
            root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
            root.setMaxQueuedQueries(queries);
            root.setMaxRunningQueries(queries);
            ResourceGroup group = root;
            for (int i = 0; i < children; i++) {
                group = root.getOrCreateSubGroup(String.valueOf(i));
                group.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
                group.setMaxQueuedQueries(queries);
                group.setMaxRunningQueries(queries);
            }
            for (int i = 0; i < queries; i++) {
                group.add(new MockQueryExecution(10));
            }
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
        }

        public RootResourceGroup getRoot()
        {
            return root;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkResourceGroup.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
