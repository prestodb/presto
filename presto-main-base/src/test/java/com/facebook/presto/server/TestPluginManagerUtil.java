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
package com.facebook.presto.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.server.PluginManagerUtil.selectPluginJars;
import static org.testng.Assert.assertEquals;

public class TestPluginManagerUtil
{
    @Test
    public void testSelectPluginJars()
    {
        Path dirPath = Paths.get("/plugin/hive");
        File dir = dirPath.toFile();
        File a = dirPath.resolve("a.jar").toFile();
        File b = dirPath.resolve("b.jar").toFile();
        List<File> files = ImmutableList.of(a, b);

        assertEquals(selectPluginJars(dir, files, ImmutableSet.of()), files);
        assertEquals(selectPluginJars(dir, files, ImmutableSet.of("b.jar", "other.jar")), ImmutableList.of(a));
        assertEquals(selectPluginJars(dir, files, ImmutableSet.of("other.jar")), files);
        assertEquals(selectPluginJars(dir, ImmutableList.of(), ImmutableSet.of("a.jar")), ImmutableList.of());
    }

    @Test
    public void testSelectPluginJarsNeverEmptiesADirectory()
    {
        Path dirPath = Paths.get("/plugin/kafka");
        File dir = dirPath.toFile();
        List<File> files = ImmutableList.of(dirPath.resolve("presto-kafka.jar").toFile(), dirPath.resolve("guava.jar").toFile());

        // excluding every jar would leave no Plugin to find: load the directory whole instead
        assertEquals(selectPluginJars(dir, files, ImmutableSet.of("presto-kafka.jar", "guava.jar")), files);

        // ... even when a non-jar file would survive the exclusions
        List<File> withNonJar = ImmutableList.<File>builder().addAll(files).add(dirPath.resolve("README.txt").toFile()).build();
        assertEquals(selectPluginJars(dir, withNonJar, ImmutableSet.of("presto-kafka.jar", "guava.jar")), withNonJar);
    }
}
