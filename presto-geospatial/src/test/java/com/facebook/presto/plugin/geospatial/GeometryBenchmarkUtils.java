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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Random;

import static com.google.common.io.Resources.readLines;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class GeometryBenchmarkUtils
{
    private GeometryBenchmarkUtils() {}

    public static String loadPolygon(String path)
            throws IOException
    {
        URL resource = requireNonNull(GeometryBenchmarkUtils.class.getClassLoader().getResource(path), "resource not found: " + path);
        List<String> lines = readLines(resource, UTF_8);
        String line = lines.get(0);
        String[] parts = line.split("\\|");
        return parts[0];
    }

    static double[] createCircleCoordinates(int numVertices)
    {
        double[] coordinates = new double[2 * numVertices + 2];
        for (int i = 0; i < numVertices; i++) {
            double angle = i * Math.PI * 2 / numVertices;
            coordinates[2 * i] = Math.cos(angle);
            coordinates[2 * i + 1] = Math.sin(angle);
        }
        coordinates[2 * numVertices] = 1;
        coordinates[2 * numVertices + 1] = 0;
        return coordinates;
    }

    static double[] createRandomCoordinates(int numPoints)
    {
        int seed = 133;
        double min = 0;
        double max = 5;
        return new Random(seed)
                .doubles(2 * numPoints, min, max)
                .toArray();
    }

    /**
     * Create a set of coordinates that are monotonically increasing in y,
     * but random in x.
     */
    static double[] createAccordionCoordinates(int numPoints)
    {
        int seed = 4880;
        double min = 0;
        double max = 5;
        double[] coordinates = new double[2 * numPoints];
        Random random = new Random(seed);
        for (int i = 0; i < numPoints; i++) {
            coordinates[2 * i] = random.nextDouble();
            coordinates[2 * i + 1] = i * (max - min) / numPoints + min;
        }
        return coordinates;
    }

    static String createCoordinateString(double... coordinates)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        String separator = "";
        for (int i = 0; i < coordinates.length; i++) {
            if (i % 2 == 0) {
                sb.append(separator);
                separator = ",";
            }
            else {
                sb.append(" ");
            }
            sb.append(coordinates[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    static String createCirclePolygon(int numVertices)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON (");
        sb.append(createCoordinateString(createCircleCoordinates(numVertices)));
        sb.append(")");
        return sb.toString();
    }
}
