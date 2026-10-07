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
package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.classloader_interface.SparkProcessType.DRIVER;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.EXECUTOR;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.LOCAL_EXECUTOR;
import static com.facebook.presto.spark.launcher.ExecutorClasspathManifest.CONFIG_PROPERTY;
import static com.facebook.presto.spark.launcher.ExecutorClasspathManifest.PLUGIN_EXCLUDED_JARS;
import static com.facebook.presto.spark.launcher.ExecutorClasspathManifest.Section.LIB;
import static com.facebook.presto.spark.launcher.ExecutorClasspathManifest.Section.PLUGIN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

// single-threaded: testLoadFailsOpenOnStderr swaps System.err
@Test(singleThreaded = true)
public class TestExecutorClasspathManifest
{
    @Test
    public void testParse()
    {
        ExecutorClasspathManifest manifest = parse(
                "# header",
                "",
                "[lib-exclude]",
                "a.jar",
                "  b.jar  # why",
                "lib/c.jar",
                "[plugin-exclude]   # trailing comment",
                "d.jar");
        assertEquals(manifest.excludedJars(LIB), ImmutableSet.of("a.jar", "b.jar", "c.jar"));
        assertEquals(manifest.excludedJars(PLUGIN), ImmutableSet.of("d.jar"));
        assertTrue(manifest.isRestricted());
    }

    @Test
    public void testMissingOrEmptySectionExcludesNothing()
    {
        ExecutorClasspathManifest pluginOnly = parse("[plugin-exclude]", "a.jar");
        assertEquals(pluginOnly.excludedJars(LIB), ImmutableSet.of());
        assertEquals(pluginOnly.excludedJars(PLUGIN), ImmutableSet.of("a.jar"));

        ExecutorClasspathManifest empty = parse("[lib-exclude]", "[plugin-exclude]");
        assertEquals(empty.excludedJars(LIB), ImmutableSet.of());
        assertEquals(empty.excludedJars(PLUGIN), ImmutableSet.of());
        assertFalse(empty.isRestricted());

        assertFalse(parse("# only comments", "").isRestricted());
    }

    @Test
    public void testRepeatedSectionAccumulates()
    {
        ExecutorClasspathManifest manifest = parse("[lib-exclude]", "a.jar", "[plugin-exclude]", "b.jar", "[lib-exclude]", "c.jar");
        assertEquals(manifest.excludedJars(LIB), ImmutableSet.of("a.jar", "c.jar"));
        assertEquals(manifest.excludedJars(PLUGIN), ImmutableSet.of("b.jar"));
    }

    @Test
    public void testMalformed()
    {
        assertMalformed("line 1: a.jar appears before any section", "a.jar", "[lib-exclude]");
        assertMalformed("line 2: unknown section [jars]", "[lib-exclude]", "[jars]");
        // the old keep-list headers are not accepted: an old manifest fails open instead of being misread
        assertMalformed("line 1: unknown section [lib]", "[lib]", "a.jar");
        assertMalformed("line 1: expected [section] or a .jar file name: [plugin-exclude", "[plugin-exclude", "a.jar");
        assertMalformed("line 2: expected [section] or a .jar file name: notes.txt", "[lib-exclude]", "notes.txt");
        assertMalformed("line 2: expected [section] or a .jar file name: a.jar b.jar", "[lib-exclude]", "a.jar b.jar");
        assertMalformed("line 2: expected [section] or a .jar file name: a,b.jar", "[plugin-exclude]", "a,b.jar");
        assertMalformed("line 1: unknown section [lib-excluded]", "[lib-excluded]", "a.jar");
        assertMalformed("line 1: expected [section] or a .jar file name: [LIB-EXCLUDE]", "[LIB-EXCLUDE]", "a.jar");
    }

    @Test
    public void testLoadFailsOpenOnStderr()
            throws IOException
    {
        assertSame(ExecutorClasspathManifest.load(Optional.empty()), ExecutorClasspathManifest.UNRESTRICTED);

        Path directory = Files.createTempDirectory("executor-classpath");
        Path malformed = directory.resolve("malformed.txt");
        Path unreadable = directory.resolve("unreadable.txt");
        Path valid = directory.resolve("valid.txt");
        try {
            // missing file: full classpath, and stderr names the file
            File missing = directory.resolve("missing.txt").toFile();
            String missingErr = stderrOf(() -> assertSame(ExecutorClasspathManifest.load(Optional.of(missing)), ExecutorClasspathManifest.UNRESTRICTED));
            assertTrue(missingErr.contains("could not use executor classpath manifest " + missing), missingErr);
            assertTrue(missingErr.contains("using the full classpath"), missingErr);

            // malformed line: full classpath, and stderr gives the line and what is wrong with it
            Files.write(malformed, ImmutableList.of("[lib-exclude]", "a.jar", "notes.txt"), UTF_8);
            String malformedErr = stderrOf(() -> assertSame(ExecutorClasspathManifest.load(Optional.of(malformed.toFile())), ExecutorClasspathManifest.UNRESTRICTED));
            assertTrue(malformedErr.contains("using the full classpath"), malformedErr);
            assertTrue(malformedErr.contains("line 3: expected [section] or a .jar file name: notes.txt"), malformedErr);

            // unreadable file: full classpath (skipped where permissions do not apply, e.g. as root)
            Files.write(unreadable, ImmutableList.of("[lib-exclude]", "a.jar"), UTF_8);
            Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
            if (!Files.isReadable(unreadable)) {
                String unreadableErr = stderrOf(() -> assertSame(ExecutorClasspathManifest.load(Optional.of(unreadable.toFile())), ExecutorClasspathManifest.UNRESTRICTED));
                assertTrue(unreadableErr.contains("could not use executor classpath manifest " + unreadable.toFile()), unreadableErr);
                assertTrue(unreadableErr.contains("using the full classpath"), unreadableErr);
            }

            // the fallback excludes nothing anywhere and hands nothing to plugins
            ExecutorClasspathManifest fallback = ExecutorClasspathManifest.UNRESTRICTED;
            assertEquals(fallback.excludedJars(LIB), ImmutableSet.of());
            assertEquals(fallback.excludedJars(PLUGIN), ImmutableSet.of());
            assertFalse(fallback.serviceConfig(ImmutableMap.of()).containsKey(PLUGIN_EXCLUDED_JARS));

            // a valid file loads
            Files.write(valid, ImmutableList.of("[lib-exclude]", "a.jar"), UTF_8);
            assertEquals(ExecutorClasspathManifest.load(Optional.of(valid.toFile())).excludedJars(LIB), ImmutableSet.of("a.jar"));
        }
        finally {
            Files.deleteIfExists(malformed);
            Files.deleteIfExists(unreadable);
            Files.deleteIfExists(valid);
            Files.delete(directory);
        }
    }

    @Test
    public void testJavaEngineRefusedOnNarrowedExecutor()
    {
        IllegalStateException refused = expectThrows(IllegalStateException.class, () -> ExecutorClasspathManifest.checkJavaEngineAllowed(true));
        assertEquals(refused.getMessage(),
                "this executor's classpath was narrowed (by presto.spark.executor-classpath-manifest or plugin.excluded-jars), "
                        + "but the query runs on the Java engine (native_execution_enabled=false); "
                        + "keep native execution enabled for this query, or remove those settings");

        // nothing actually excluded: the Java engine is allowed
        ExecutorClasspathManifest.checkJavaEngineAllowed(false);
    }

    @Test
    public void testPluginExclusions()
    {
        assertEquals(ExecutorClasspathManifest.pluginExclusions(ImmutableMap.of()), ImmutableSet.of());
        assertEquals(ExecutorClasspathManifest.pluginExclusions(ImmutableMap.of(PLUGIN_EXCLUDED_JARS, " a.jar , b.jar ,")), ImmutableSet.of("a.jar", "b.jar"));
        // the effective exclusions include a plugin.excluded-jars set directly, with no manifest
        Map<String, String> direct = ExecutorClasspathManifest.UNRESTRICTED.serviceConfig(ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "a.jar"));
        assertEquals(ExecutorClasspathManifest.pluginExclusions(direct), ImmutableSet.of("a.jar"));
    }

    @Test
    public void testPluginExclusionsApplied()
            throws IOException
    {
        Path plugins = Files.createTempDirectory("plugins");
        Path hive = Files.createDirectory(plugins.resolve("hive"));
        Path tpch = Files.createDirectory(plugins.resolve("tpch"));
        List<Path> files = ImmutableList.of(
                Files.createFile(hive.resolve("presto-hive.jar")),
                Files.createFile(hive.resolve("guava.jar")),
                Files.createFile(tpch.resolve("presto-tpch.jar")),
                Files.createFile(tpch.resolve("README.txt")));
        try {
            File dir = plugins.toFile();
            assertFalse(ExecutorClasspathManifest.pluginExclusionsApplied(dir, ImmutableSet.of()));
            // a listed jar no plugin directory contains excludes nothing
            assertFalse(ExecutorClasspathManifest.pluginExclusionsApplied(dir, ImmutableSet.of("absent.jar")));
            // hive keeps presto-hive.jar after losing guava.jar: applied
            assertTrue(ExecutorClasspathManifest.pluginExclusionsApplied(dir, ImmutableSet.of("guava.jar")));
            // tpch would keep only README.txt, so it is loaded whole: not applied
            assertFalse(ExecutorClasspathManifest.pluginExclusionsApplied(dir, ImmutableSet.of("presto-tpch.jar")));
            // unreadable plugin directory: any exclusion counts as applied
            assertTrue(ExecutorClasspathManifest.pluginExclusionsApplied(new File(dir, "missing"), ImmutableSet.of("guava.jar")));
        }
        finally {
            for (Path file : files) {
                Files.delete(file);
            }
            Files.delete(hive);
            Files.delete(tpch);
            Files.delete(plugins);
        }
    }

    @Test
    public void testConfiguredFileGates()
    {
        Map<String, String> config = ImmutableMap.of(CONFIG_PROPERTY, "executor-classpath.txt", "native-execution-enabled", "true");
        assertEquals(configuredFile(EXECUTOR, config, true), Optional.of(new File("/pkg", "executor-classpath.txt")));

        assertEquals(configuredFile(EXECUTOR, ImmutableMap.of("native-execution-enabled", "true"), true), Optional.empty());
        assertEquals(configuredFile(EXECUTOR, ImmutableMap.of(CONFIG_PROPERTY, " ", "native-execution-enabled", "true"), true), Optional.empty());
        assertEquals(configuredFile(DRIVER, config, true), Optional.empty());
        assertEquals(configuredFile(LOCAL_EXECUTOR, config, true), Optional.empty());
        assertEquals(configuredFile(EXECUTOR, ImmutableMap.of(CONFIG_PROPERTY, "executor-classpath.txt"), true), Optional.empty());
        assertEquals(configuredFile(EXECUTOR, config, false), Optional.empty());
    }

    @Test
    public void testConfiguredFileAbsolutePath()
    {
        Map<String, String> config = ImmutableMap.of(CONFIG_PROPERTY, "/etc/presto/manifest.txt", "native-execution-enabled", "true");
        assertEquals(configuredFile(EXECUTOR, config, true), Optional.of(new File("/etc/presto/manifest.txt")));
    }

    @Test
    public void testServiceConfig()
    {
        Map<String, String> launcherConfig = ImmutableMap.of(CONFIG_PROPERTY, "executor-classpath.txt", "query.max-memory", "1GB");

        // the launcher's own key never reaches Presto, which would reject it as unused
        assertEquals(ExecutorClasspathManifest.UNRESTRICTED.serviceConfig(launcherConfig), ImmutableMap.of("query.max-memory", "1GB"));

        Map<String, String> config = parse("[plugin-exclude]", "a.jar").serviceConfig(launcherConfig);
        assertEquals(config, ImmutableMap.of("query.max-memory", "1GB", PLUGIN_EXCLUDED_JARS, "a.jar"));

        // a lib-only manifest leaves plugin loading alone
        assertEquals(parse("[lib-exclude]", "a.jar").serviceConfig(launcherConfig), ImmutableMap.of("query.max-memory", "1GB"));

        // the exclude list replaces a value already in the config
        Map<String, String> withExisting = ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "stale.jar");
        assertEquals(parse("[plugin-exclude]", "a.jar").serviceConfig(withExisting), ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "a.jar"));
    }

    private static Optional<File> configuredFile(SparkProcessType processType, Map<String, String> config, boolean nativeWorkerConfigured)
    {
        return ExecutorClasspathManifest.configuredFile(processType, config, nativeWorkerConfigured, "/pkg");
    }

    private static ExecutorClasspathManifest parse(String... lines)
    {
        return ExecutorClasspathManifest.parse(ImmutableList.copyOf(lines));
    }

    private static void assertMalformed(String message, String... lines)
    {
        List<String> manifest = ImmutableList.copyOf(lines);
        assertEquals(expectThrows(IllegalArgumentException.class, () -> ExecutorClasspathManifest.parse(manifest)).getMessage(), message);
    }

    private static String stderrOf(Runnable action)
    {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            action.run();
        }
        finally {
            System.setErr(original);
        }
        return captured.toString();
    }
}
