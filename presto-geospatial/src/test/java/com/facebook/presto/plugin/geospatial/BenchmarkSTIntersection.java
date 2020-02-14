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
package com.facebook.presto.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.expandEnvelope;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stIntersection;
import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;
import static io.airlift.slice.Slices.utf8Slice;

@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSTIntersection
{
    @Benchmark
    public Object stIntersectionSimplePolygons(BenchmarkData data)
    {
        return stIntersection(data.simplePolygon, data.simplePolygon);
    }

    @Benchmark
    public Object stIntersectionComplexPolygons(BenchmarkData data)
    {
        return stIntersection(data.complexPolygon, data.complexPolygon);
    }

    @Benchmark
    public Object stIntersectionSimpleComplexPolygons(BenchmarkData data)
    {
        return stIntersection(data.simplePolygon, data.complexPolygon);
    }

    @Benchmark
    public Object stIntersectionSimplePolygonSmallEnvelope(BenchmarkData data)
    {
        return stIntersection(data.simplePolygon, data.smallEnvelope);
    }

    @Benchmark
    public Object stIntersectionSimplePolygonLargeEnvelope(BenchmarkData data)
    {
        return stIntersection(data.simplePolygon, data.largeEnvelope);
    }

    @Benchmark
    public Object stIntersectionComplexPolygonSmallEnvelope(BenchmarkData data)
    {
        return stIntersection(data.complexPolygon, data.smallEnvelope);
    }

    @Benchmark
    public Object stIntersectionComplexPolygonLargeEnvelope(BenchmarkData data)
    {
        return stIntersection(data.complexPolygon, data.largeEnvelope);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice simplePolygon;
        private Slice complexPolygon;
        private Slice largeEnvelope;
        private Slice smallEnvelope;

        @Setup
        public void setup()
                throws IOException
        {
            simplePolygon = stGeometryFromText(utf8Slice("POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))"));
            complexPolygon = stGeometryFromText(utf8Slice(loadPolygon("large_polygon.txt")));
            largeEnvelope = expandEnvelope(complexPolygon, 1.0);
            Envelope envelope = deserializeEnvelope(complexPolygon);
            double deltaX = envelope.getXMax() - envelope.getXMin();
            double deltaY = envelope.getYMax() - envelope.getYMin();
            smallEnvelope = EsriGeometrySerde.serialize(new Envelope(
                    envelope.getXMin() + deltaX / 4,
                    envelope.getYMin() + deltaY / 4,
                    envelope.getXMax() - deltaX / 4,
                    envelope.getYMax() - deltaY / 4));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSTIntersection.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
