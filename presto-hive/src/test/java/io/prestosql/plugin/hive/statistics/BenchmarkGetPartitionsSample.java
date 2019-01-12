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
package io.prestosql.plugin.hive.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.spi.connector.SchemaTableName;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.getPartitionsSample;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGetPartitionsSample
{
    private static final int TOTAL_SIZE = 10_000;
    private static final int SAMPLE_SIZE = 100;

    @Benchmark
    public List<HivePartition> selectSample(BenchmarkData data)
    {
        return getPartitionsSample(data.partitions, SAMPLE_SIZE);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        public List<HivePartition> partitions;

        @Setup
        public void setup()
        {
            ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();
            SchemaTableName table = new SchemaTableName("schema", "table");
            for (int i = 0; i < TOTAL_SIZE; i++) {
                partitions.add(new HivePartition(table, "partition_" + i, ImmutableMap.of()));
            }
            this.partitions = partitions.build();
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGetPartitionsSample.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
