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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGeometryToBingTiles
{
    @Benchmark
    public Object geometryToBingTiles(BenchmarkData data)
    {
        return BingTileFunctions.geometryToBingTiles(data.geometry, data.zoomLevel);
    }

    @Benchmark
    public Object envelopeToBingTiles(BenchmarkData data)
    {
        return BingTileFunctions.geometryToBingTiles(data.envelope, data.zoomLevel);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice geometry;
        private Slice envelope;
        private int zoomLevel;

        @Setup
        public void setup()
                throws IOException
        {
            Path filePath = Paths.get(this.getClass().getClassLoader().getResource("large_polygon.txt").getPath());
            List<String> lines = Files.readAllLines(filePath);
            String line = lines.get(0);
            String[] parts = line.split("\\|");
            String wkt = parts[0];
            geometry = GeoFunctions.stGeometryFromText(Slices.utf8Slice(wkt));
            envelope = GeoFunctions.stEnvelope(geometry);
            zoomLevel = Integer.parseInt(parts[1]);
        }
    }

    public static void main(String[] args)
            throws IOException, RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkGeometryToBingTiles().geometryToBingTiles(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGeometryToBingTiles.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
