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
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import io.airlift.slice.Slices;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;

public class ContainsRunner
{
    private ContainsRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        String smallWkt = loadPolygon("93_vertexes.txt");
        String mediumWkt = loadPolygon("758_vertexes.txt");
        String largeWkt = loadPolygon("1486_vertexes.txt");
        String xlargeWkt = loadPolygon("9533_vertexes.txt");

        OGCGeometry smallGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(smallWkt)));
        OGCGeometry mediumGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(mediumWkt)));
        OGCGeometry largeGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(largeWkt)));
        OGCGeometry xlargeGeometry = deserialize(GeoFunctions.stGeometryFromText(Slices.utf8Slice(xlargeWkt)));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        Runnable runnable = () -> {
            int innerPoints = 0;
            Envelope envelope = getEnvelope(xlargeGeometry);
            while (true) {
                if (xlargeGeometry.contains(getRandomPoint(envelope))) {
                    innerPoints++;
                    if (innerPoints % 1_000_000 == 0) {
                        System.out.println("Inner points: " + innerPoints);
                    }
                }
            }
        };

        for (int i = 0; i < 4; i++) {
            scheduler.schedule(runnable, 0, TimeUnit.SECONDS);
        }

//        System.out.println("Inner points: " + innerPoints);
    }

    private static Envelope getEnvelope(OGCGeometry geometry)
    {
        Envelope env = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(env);
        return env;
    }

    private static OGCPoint getRandomPoint(Envelope envelope)
    {
        double x = Math.random() * envelope.getWidth() + envelope.getXMin();
        double y = Math.random() * envelope.getHeight() + envelope.getYMin();
        return new OGCPoint(new Point(x, y), null);
    }
}
