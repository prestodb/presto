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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.geospatial.GeoFunctions.stContains;
import static io.prestosql.plugin.geospatial.GeoFunctions.stEnvelope;
import static io.prestosql.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static io.prestosql.plugin.geospatial.GeoFunctions.stIntersects;
import static io.prestosql.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;

@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkSTIntersects
{
    @Benchmark
    public Object stIntersectsInnerLine(BenchmarkData data)
    {
        return stIntersects(data.innerLine, data.geometry);
    }

    @Benchmark
    public Object stIntersectsInnerLineSimpleGeometry(BenchmarkData data)
    {
        return stIntersects(data.innerLine, data.simpleGeometry);
    }

    @Benchmark
    public Object stIntersectsCrossingLine(BenchmarkData data)
    {
        return stIntersects(data.crossingLine, data.geometry);
    }

    @Benchmark
    public Object stIntersectsCrossingLineSimpleGeometry(BenchmarkData data)
    {
        return stIntersects(data.crossingLine, data.simpleGeometry);
    }

    @Benchmark
    public Object stIntersectsOuterLineInEnvelope(BenchmarkData data)
    {
        return stIntersects(data.outerLineInEnvelope, data.geometry);
    }

    @Benchmark
    public Object stIntersectsOuterLineInEnvelopeSimpleGeometry(BenchmarkData data)
    {
        return stIntersects(data.outerLineInEnvelope, data.simpleGeometry);
    }

    @Benchmark
    public Object stIntersectsOuterLineNotInEnvelope(BenchmarkData data)
    {
        return stIntersects(data.outerLineNotInEnvelope, data.geometry);
    }

    @Benchmark
    public Object stIntersectsOuterLineNotInEnvelopeSimpleGeometry(BenchmarkData data)
    {
        return stIntersects(data.outerLineNotInEnvelope, data.simpleGeometry);
    }

    @Test
    public void validateBenchmarkData()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        data.validate();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice simpleGeometry;
        private Slice geometry;
        private Slice innerLine;
        private Slice crossingLine;
        private Slice outerLineInEnvelope;
        private Slice outerLineNotInEnvelope;

        @Setup
        public void setup()
                throws IOException
        {
            simpleGeometry = stGeometryFromText(utf8Slice("POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54))"));
            geometry = stGeometryFromText(utf8Slice(loadPolygon("large_polygon.txt")));
            innerLine = stGeometryFromText(utf8Slice("LINESTRING (16.6 54.0167, 16.6 54.017)"));
            crossingLine = stGeometryFromText(utf8Slice("LINESTRING (16.6 53, 16.6 56)"));
            outerLineInEnvelope = stGeometryFromText(utf8Slice("LINESTRING (16.6667 54.05, 16.8667 54.05)"));
            outerLineNotInEnvelope = stGeometryFromText(utf8Slice("LINESTRING (16.6667 54.25, 16.8667 54.25)"));
        }

        public void validate()
        {
            validate(simpleGeometry);
            validate(geometry);
        }

        public void validate(Slice geometry)
        {
            Slice envelope = stEnvelope(geometry);

            // innerLine
            verify(stIntersects(geometry, innerLine));
            verify(stContains(geometry, innerLine));

            // crossingLine
            verify(stIntersects(geometry, crossingLine));
            verify(!stContains(geometry, crossingLine));

            // outerLineInEnvelope
            verify(!stIntersects(geometry, outerLineInEnvelope));
            verify(stIntersects(envelope, outerLineInEnvelope));

            // outerLineNotInEnvelope
            verify(!stIntersects(geometry, outerLineNotInEnvelope));
            verify(!stIntersects(envelope, outerLineNotInEnvelope));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSTIntersects.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
