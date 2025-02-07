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
package com.facebook.presto.iceberg;

import com.facebook.presto.tests.DistributedQueryRunner;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

@State(Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkIcebergHadoopCatalog
        extends IcebergBaseBenchmark
{
    @Override
    public DistributedQueryRunner getQueryRunner()
    {
        try {
            return IcebergQueryRunner.builder()
                    .setCatalogType(HADOOP)
                    .setNodeCount(OptionalInt.of(4))
                    .build()
                    .getQueryRunner();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkIcebergHadoopCatalog.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
