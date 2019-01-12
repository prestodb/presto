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
package io.prestosql.geospatial.serde;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BenchmarkGeometrySerializationData
{
    public static final String POINT = "POINT (-2e3 -4e33)";
    public static final String MULTIPOINT = "MULTIPOINT (-2e3 -4e33, 0 0, 1 1, 2 3)";
    public static final String LINESTRING = "LINESTRING (-2e3 -4e33, 0 0, 1 1, 2 3)";
    public static final String MULTILINESTRING = "MULTILINESTRING ((-2e3 -4e33, 0 0, 1 1, 2 3), (0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7))";
    public static final String POLYGON = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
    public static final String MULTIPOLYGON = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 40, 10 20, 30 10)))";
    public static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION (" +
            "POINT (-2e3 -4e33), " +
            "MULTIPOINT (-2e3 -4e33, 0 0, 1 1, 2 3), " +
            "LINESTRING (-2e3 -4e33, 0 0, 1 1, 2 3), " +
            "MULTILINESTRING ((-2e3 -4e33, 0 0, 1 1, 2 3), (0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7)), " +
            "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)), " +
            "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 40, 10 20, 30 10))))";

    private BenchmarkGeometrySerializationData() {}

    public static String readResource(String resource)
    {
        try {
            return Resources.toString(getResource(resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
