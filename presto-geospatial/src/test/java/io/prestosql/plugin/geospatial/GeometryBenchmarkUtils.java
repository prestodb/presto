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

import java.io.IOException;
import java.net.URL;
import java.util.List;

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
}
