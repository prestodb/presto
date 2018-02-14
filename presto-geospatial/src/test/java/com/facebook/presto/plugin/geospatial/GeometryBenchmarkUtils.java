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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class GeometryBenchmarkUtils
{
    private GeometryBenchmarkUtils() {}

    public static String loadPolygon(String fileName)
            throws IOException
    {
        Path filePath = Paths.get(GeometryBenchmarkUtils.class.getClassLoader().getResource(fileName).getPath());
        List<String> lines = Files.readAllLines(filePath);
        String line = lines.get(0);
        String[] parts = line.split("\\|");
        return parts[0];
    }
}
