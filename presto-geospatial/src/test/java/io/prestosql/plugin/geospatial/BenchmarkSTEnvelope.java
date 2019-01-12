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
package io.prestosql.plugin.geospatial;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static io.prestosql.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSTEnvelope
{
    @Benchmark
    public Slice simpleGeometry(BenchmarkData data)
    {
        return GeoFunctions.stEnvelope(data.simpleGeometry);
    }

    @Benchmark
    public Slice complexGeometry(BenchmarkData data)
    {
        return GeoFunctions.stEnvelope(data.complexGeometry);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice complexGeometry;
        private Slice simpleGeometry;

        @Setup
        public void setup()
                throws IOException
        {
            complexGeometry = GeoFunctions.stGeometryFromText(Slices.utf8Slice(loadPolygon("large_polygon.txt")));
            simpleGeometry = GeoFunctions.stGeometryFromText(Slices.utf8Slice("POLYGON ((1 1, 4 1, 1 4))"));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSTEnvelope.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
