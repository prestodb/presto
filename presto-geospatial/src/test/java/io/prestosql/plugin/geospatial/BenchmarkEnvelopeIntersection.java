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
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.geospatial.serde.GeometrySerde.deserialize;
import static io.prestosql.plugin.geospatial.GeoFunctions.stEnvelope;
import static io.prestosql.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static io.prestosql.plugin.geospatial.GeoFunctions.stIntersection;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkEnvelopeIntersection
{
    @Benchmark
    public Slice envelopes(BenchmarkData data)
    {
        return stIntersection(data.envelope, data.otherEnvelope);
    }

    @Benchmark
    public Slice geometries(BenchmarkData data)
    {
        return stIntersection(data.geometry, data.otherGeometry);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice envelope;
        private Slice otherEnvelope;

        private Slice geometry;
        private Slice otherGeometry;

        @Setup
        public void setup()
        {
            geometry = stGeometryFromText(utf8Slice("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"));
            otherGeometry = stGeometryFromText(utf8Slice("POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))"));
            envelope = stEnvelope(geometry);
            otherEnvelope = stEnvelope(otherGeometry);
        }
    }

    @Test
    public void validate()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkEnvelopeIntersection benchmark = new BenchmarkEnvelopeIntersection();
        assertEquals(deserialize(benchmark.envelopes(data)), deserialize(benchmark.geometries(data)));
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkEnvelopeIntersection.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
