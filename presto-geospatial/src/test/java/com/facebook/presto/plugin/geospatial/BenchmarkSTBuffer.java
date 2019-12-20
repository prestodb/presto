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

import static com.facebook.presto.plugin.geospatial.GeoFunctions.stBuffer;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;
import static io.airlift.slice.Slices.utf8Slice;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSTBuffer
{
    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSTBuffer.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @Benchmark
    public Object stBufferPoint(BenchmarkData data)
    {
        return stBuffer(data.point, 0.1);
    }

    @Benchmark
    public Object stBufferMultiPointSparse(BenchmarkData data)
    {
        return stBuffer(data.multiPointSparse, 0.1);
    }

    @Benchmark
    public Object stBufferMultiPointDense(BenchmarkData data)
    {
        return stBuffer(data.multiPointDense, 0.1);
    }

    @Benchmark
    public Object stBufferMultiPointReallyDense(BenchmarkData data)
    {
        return stBuffer(data.multiPointReallyDense, 0.1);
    }

    @Benchmark
    public Object stBufferLineStringCircle(BenchmarkData data)
    {
        return stBuffer(data.lineStringCircle, 0.1);
    }

    @Benchmark
    public Object stBufferLineStringDense(BenchmarkData data)
    {
        return stBuffer(data.lineStringDense, 0.1);
    }

    @Benchmark
    public Object stBufferPolygonSimple(BenchmarkData data)
    {
        return stBuffer(data.polygonSimple, 0.1);
    }

    @Benchmark
    public Object stBufferPolygonNormal(BenchmarkData data)
    {
        return stBuffer(data.polygonNormal, 0.1);
    }

    @Benchmark
    public Object stBufferPolygonDense(BenchmarkData data)
    {
        return stBuffer(data.polygonComplex, 0.1);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice point;
        private Slice multiPointSparse;
        private Slice multiPointDense;
        private Slice multiPointReallyDense;
        private Slice lineStringCircle;
        private Slice lineStringDense;
        private Slice polygonSimple;
        private Slice polygonNormal;
        private Slice polygonComplex;

        @Setup
        public void setup()
                throws IOException
        {
            point = GeoFunctions.stPoint(0, 0);
            multiPointSparse = stGeometryFromText(utf8Slice(
                    "MULTIPOINT " + GeometryBenchmarkUtils.createCoordinateString(GeometryBenchmarkUtils.createRandomCoordinates(50))));
            multiPointDense = stGeometryFromText(utf8Slice(
                    "MULTIPOINT " + GeometryBenchmarkUtils.createCoordinateString(GeometryBenchmarkUtils.createRandomCoordinates(500))));
            multiPointReallyDense = stGeometryFromText(utf8Slice(
                    "MULTIPOINT " + GeometryBenchmarkUtils.createCoordinateString(GeometryBenchmarkUtils.createRandomCoordinates(5000))));
            lineStringCircle = stGeometryFromText(utf8Slice(
                    "LINESTRING " + GeometryBenchmarkUtils.createCoordinateString(GeometryBenchmarkUtils.createCircleCoordinates(1000))));
            lineStringDense = stGeometryFromText(utf8Slice(
                    "LINESTRING " + GeometryBenchmarkUtils.createCoordinateString(GeometryBenchmarkUtils.createAccordionCoordinates(1000))));
            polygonSimple = stGeometryFromText(utf8Slice(GeometryBenchmarkUtils.createCirclePolygon(8)));
            polygonNormal = stGeometryFromText(utf8Slice(loadPolygon("large_polygon.txt")));
            polygonComplex = stGeometryFromText(utf8Slice(GeometryBenchmarkUtils.createCirclePolygon(500000)));
        }
    }
}
