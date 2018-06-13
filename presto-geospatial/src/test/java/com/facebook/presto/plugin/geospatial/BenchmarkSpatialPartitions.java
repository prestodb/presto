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
import com.esri.core.geometry.Envelope2D;
import com.facebook.presto.geospatial.KDBTree;
import com.facebook.presto.geospatial.KDBTreeSpec;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import io.airlift.json.ObjectMapperProvider;
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
import java.net.URL;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.spatialPartitions;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stPoint;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@OutputTimeUnit(SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkSpatialPartitions
{
    @Benchmark
    public Block benchmark(BenchmarkData data)
    {
        return spatialPartitions(data.kdbTreeHash, data.kdbTreeJson, data.point);
    }

    @Benchmark
    public Block benchmarkRadius(BenchmarkData data)
    {
        return spatialPartitions(data.kdbTreeHash, data.kdbTreeJson, data.point, 0.1);
    }

    @Benchmark
    public Envelope deserialize(BenchmarkData data)
    {
        return deserializeEnvelope(data.point);
    }

    @Benchmark
    public Object kdbTreeLookup(BenchmarkData data)
    {
        return data.kdbTree.findLeafs(data.envelope);
    }

    @Benchmark
    public KDBTree getKdbTree(BenchmarkData data)
    {
        return GeoFunctions.getKdbTree(data.kdbTreeHash, data.kdbTreeJson);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice kdbTreeJson;
        private Slice point;
        private KDBTree kdbTree;
        private Envelope2D envelope;
        private long kdbTreeHash;

        @Setup
        public void setup()
                throws IOException
        {
            URL resource = requireNonNull(TestGeoFunctions.class.getClassLoader().getResource("kdb_tree.json"), "resource not found: kdb_tree.json");
            String rawJson = Resources.toString(resource, UTF_8);
            kdbTreeJson = utf8Slice(rawJson);
            point = stPoint(-72.67913, 40.775191);
            kdbTree = getKdbTree(rawJson);
            envelope = new Envelope2D(-72.67913, 40.775191, -72.67913, 40.775191);
            kdbTreeHash = Hashing.sha256().newHasher().putBytes(kdbTreeJson.getBytes()).hash().asLong();
        }

        private static KDBTree getKdbTree(String json)
        {
            ObjectMapper objectMapper = new ObjectMapperProvider().get();
            try {
                return KDBTree.fromSpec(objectMapper.readValue(json, KDBTreeSpec.class));
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot parse KDB tree JSON: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSpatialPartitions.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
