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
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import io.airlift.slice.Slices;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;
import static java.lang.String.format;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
//@Threads(4)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkContains
{
    @Benchmark
    public Object containsSmall(BenchmarkData data)
    {
        return data.smallGeometry.contains(data.smallPoint);
    }

    @Benchmark
    public Object containsMedium(BenchmarkData data)
    {
        return data.mediumGeometry.contains(data.mediumPoint);
    }

    @Benchmark
    public Object containsLarge(BenchmarkData data)
    {
        return data.largeGeometry.contains(data.largePoint);
    }

    @Benchmark
    public Object containsXLarge(BenchmarkData data)
    {
        return data.xlargeGeometry.contains(data.xlargePoint);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private OGCGeometry smallGeometry;
        private OGCGeometry mediumGeometry;
        private OGCGeometry largeGeometry;
        private OGCGeometry xlargeGeometry;
        private OGCPoint smallPoint;
        private OGCPoint mediumPoint;
        private OGCPoint largePoint;
        private OGCPoint xlargePoint;

        @Param({"true", "false"})
        private boolean accelerate;

        @Setup
        public void setup()
                throws IOException
        {
            String smallWkt = loadPolygon("93_vertexes.txt");
            String mediumWkt = loadPolygon("758_vertexes.txt");
            String largeWkt = loadPolygon("1486_vertexes.txt");
            String xlargeWkt = loadPolygon("9533_vertexes.txt");

            smallGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(smallWkt)));
            mediumGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(mediumWkt)));
            largeGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(largeWkt)));
            xlargeGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(xlargeWkt)));

            if (accelerate) {
                accelerate(smallGeometry);
                accelerate(mediumGeometry);
                accelerate(largeGeometry);
                accelerate(xlargeGeometry);
            }
            System.out.println(format("Vertex counts: %s, %s, %s, %s", getPointCount(smallGeometry), getPointCount(mediumGeometry), getPointCount(largeGeometry), getPointCount(xlargeGeometry)));

            smallPoint = getInnerPoint(smallGeometry);
            mediumPoint = getInnerPoint(mediumGeometry);
            largePoint = getInnerPoint(largeGeometry);
            xlargePoint = getInnerPoint(xlargeGeometry);
        }

        private static void accelerate(OGCGeometry geometry)
        {
            boolean ok = OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate)
                    .accelerateGeometry(geometry.getEsriGeometry(), null, Geometry.GeometryAccelerationDegree.enumMild);
            System.out.println("Accelerate: " + ok);
        }

        private static int getPointCount(OGCGeometry geometry)
        {
            return ((MultiVertexGeometry) geometry.getEsriGeometry()).getPointCount();
        }

        private static OGCPoint getInnerPoint(OGCGeometry geometry)
        {
            return new OGCPoint(getEnvelope(geometry).getCenter(), null);
        }

        private static Envelope getEnvelope(OGCGeometry geometry)
        {
            Envelope env = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(env);
            return env;
        }
    }

    @Test
    public void verify()
            throws IOException
    {
        BenchmarkData data = new BenchmarkData();
        data.accelerate = true;
        data.setup();
        containsMedium(data);
    }

    @Test
    public void testAccelerate()
    {
        String wkt = "MULTIPOLYGON (((-109.642707 30.5236901, -109.607932 30.5367411, -109.5820257 30.574184, -109.5728286 30.5874766, -109.568679 30.5934741, -109.5538097 30.5918356, -109.553714 30.5918251, -109.553289 30.596034, -109.550951 30.6191889, -109.5474935 30.6221179, -109.541059 30.6275689, -109.5373751 30.6326491, -109.522538 30.6531099, -109.514671 30.6611981, -109.456764 30.6548095, -109.4556456 30.6546861, -109.4536755 30.6544688, -109.4526481 30.6543554, -109.446824 30.6537129, -109.437751 30.6702901, -109.433968 30.6709781, -109.43338 30.6774591, -109.416243 30.7164651, -109.401643 30.7230741, -109.377583 30.7145241, -109.3487939 30.7073896, -109.348594 30.7073401, -109.3483718 30.7073797, -109.3477608 30.7074887, -109.3461903 30.7078834, -109.3451022 30.7081569, -109.3431732 30.7086416, -109.3423301 30.708844, -109.3419714 30.7089301, -109.3416347 30.709011, -109.3325693 30.7111874, -109.3323814 30.7112325, -109.332233 30.7112681, -109.332191 30.7112686, -109.3247809 30.7113581, -109.322215 30.7159391, -109.327776 30.7234381, -109.350134 30.7646001, -109.364505 30.8382481, -109.410211 30.8749199, -109.400048 30.8733419, -109.3847799 30.9652412, -109.3841625 30.9689575, -109.375268 31.0224939, -109.390544 31.0227899, -109.399749 31.0363341, -109.395787 31.0468411, -109.388174 31.0810249, -109.3912446 31.0891966, -109.3913452 31.0894644, -109.392735 31.0931629, -109.4000839 31.0979214, -109.402803 31.0996821, -109.4110458 31.1034586, -109.419153 31.1071729, -109.449782 31.1279489, -109.469654 31.1159979, -109.4734874 31.1131178, -109.473753 31.1129183, -109.4739754 31.1127512, -109.491296 31.0997381, -109.507789 31.0721811, -109.512776 31.0537519, -109.5271478 31.0606861, -109.5313703 31.0627234, -109.540698 31.0672239, -109.5805468 31.0674089, -109.5807399 31.0674209, -109.595423 31.0674779, -109.60347 31.0690241, -109.6048011 31.068808, -109.6050803 31.0687627, -109.6192237 31.0664664, -109.635432 31.0638349, -109.6520068 31.0955326, -109.6522294 31.0959584, -109.652373 31.0962329, -109.657709 31.0959719, -109.718258 31.0930099, -109.821036 31.0915909, -109.8183088 31.0793374, -109.8165128 31.0712679, -109.8140062 31.0600052, -109.8138512 31.0593089, -109.812707 31.0541679, -109.8188146 31.0531909, -109.8215447 31.0527542, -109.8436765 31.0492138, -109.8514316 31.0479733, -109.8620535 31.0462742, -109.8655958 31.0457076, -109.868388 31.0452609, -109.8795483 31.0359656, -109.909274 31.0112075, -109.9210382 31.0014092, -109.9216329 31.0009139, -109.920594 30.994183, -109.9195356 30.9873254, -109.9192113 30.9852243, -109.9186281 30.9814453, -109.917814 30.9761709, -109.933894 30.9748879, -109.94094 30.9768059, -109.944854 30.9719821, -109.950803 30.9702809, -109.954025 30.9652409, -109.9584129 30.9636033, -109.958471 30.9635809, -109.9590542 30.9644372, -109.959896 30.9656733, -109.9604184 30.9664405, -109.9606288 30.9667494, -109.9608462 30.9670686, -109.961225 30.9676249, -109.9611615 30.9702903, -109.9611179 30.9721175, -109.9610885 30.9733488, -109.9610882 30.9733604, -109.9610624 30.9744451, -109.961017 30.9763469, -109.962609 30.9786559, -109.9634437 30.9783167, -110.00172 30.9627641, -110.0021152 30.9627564, -110.0224353 30.9623622, -110.0365868 30.9620877, -110.037493 30.9620701, -110.0374055 30.961663, -110.033653 30.9442059, -110.0215506 30.9492932, -110.0180392 30.9507693, -110.011203 30.9536429, -110.0062891 30.9102124, -110.0058721 30.9065268, -110.004869 30.8976609, -109.996392 30.8957129, -109.985038 30.8870439, -109.969416 30.9006011, -109.967905 30.8687239, -109.903498 30.8447749, -109.882925 30.8458289, -109.865184 30.8206519, -109.86465 30.777698, -109.864515 30.7668429, -109.837007 30.7461781, -109.83453 30.7164469, -109.839017 30.7089009, -109.813394 30.6906529, -109.808694 30.6595701, -109.795334 30.6630041, -109.7943042 30.6427223, -109.7940456 30.6376287, -109.7940391 30.637501, -109.793823 30.6332449, -109.833511 30.6274289, -109.830299 30.6252799, -109.844198 30.6254801, -109.852442 30.6056949, -109.832973 30.6021201, -109.8050409 30.591211, -109.773847 30.5790279, -109.772859 30.5521999, -109.754427 30.5393969, -109.743293 30.5443401, -109.6966136 30.5417334, -109.6648181 30.5399578, -109.6560456 30.5394679, -109.6528439 30.5392912, -109.6504039 30.5391565, -109.6473602 30.5389885, -109.646906 30.5389634, -109.6414545 30.5386625, -109.639708 30.5385661, -109.6397729 30.5382443, -109.642707 30.5236901)))";
        String pointWkt = "POINT (-109.65 31.091666666673)";

        OGCGeometry polygon = OGCGeometry.fromText(wkt);
        OGCGeometry point = OGCGeometry.fromText(pointWkt);
        System.out.println(polygon.contains(point));

        OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate)
                .accelerateGeometry(polygon.getEsriGeometry(), null, Geometry.GeometryAccelerationDegree.enumMild);
        System.out.println(polygon.contains(point));
    }

    public static void main(String[] args)
            throws IOException, RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkContains.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
