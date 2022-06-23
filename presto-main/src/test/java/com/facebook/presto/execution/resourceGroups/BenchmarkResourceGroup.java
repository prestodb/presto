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

import com.facebook.presto.execution.MockManagedQueryExecution;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup.RootInternalResourceGroup;
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

import java.util.Optional;
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
        private RootInternalResourceGroup root;

        @Setup
        public void setup()
        {
            root = new RootInternalResourceGroup("root", (group, export) -> {}, executor, ignored -> Optional.empty(), rg -> false);
            root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
            root.setMaxQueuedQueries(queries);
            root.setHardConcurrencyLimit(queries);
            InternalResourceGroup group = root;
            for (int i = 0; i < children; i++) {
                group = root.getOrCreateSubGroup(String.valueOf(i), true);
                group.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
                group.setMaxQueuedQueries(queries);
                group.setHardConcurrencyLimit(queries);
            }
            for (int i = 0; i < queries; i++) {
                group.run(new MockManagedQueryExecution(10));
            }
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
        }

        public RootInternalResourceGroup getRoot()
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
