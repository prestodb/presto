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
                "  b-[0-9].*\\.jar  # why",
                "[plugin-exclude]   # trailing comment",
                "d.jar");
        assertExcludes(manifest, LIB, "a.jar", "b-1.0.jar", "b-2.13.1.jar");
        assertNotExcludes(manifest, LIB, "d.jar", "b-api-1.0.jar", "b.jar");
        assertExcludes(manifest, PLUGIN, "d.jar");
        assertNotExcludes(manifest, PLUGIN, "a.jar", "b-1.0.jar");
        assertTrue(manifest.isRestricted());
    }

    @Test
    public void testPatternsMatchTheWholeFileName()
    {
        ExecutorClasspathManifest manifest = parse(
                "[lib-exclude]",
                "spark-core-[0-9].*\\.jar",
                "hadoop-client-runtime-[0-9].*\\.jar",
                "lucene-core-9.12.3.jar");
        // a version bump keeps matching
        assertExcludes(manifest, LIB, "spark-core-3.4.1-1.jar", "spark-core-3.4.2-1.jar", "hadoop-client-runtime-3.3.4.jar", "hadoop-client-runtime-3.4.0.jar");
        // a plain file name matches itself
        assertExcludes(manifest, LIB, "lucene-core-9.12.3.jar");
        // anchored at both ends: a similar artifact, a prefix or a suffix does not match
        assertNotExcludes(manifest, LIB,
                "spark-core_2.13-3.4.12738.jar",
                "hadoop-client-api-3.3.4.jar",
                "my-spark-core-3.4.1-1.jar",
                "spark-core-3.4.1-1.jar.bak",
                "lucene-core-9.12.4.jar");
        // a plain file name is a regular expression too: its dots match any character, except the
        // last, since only names ending in .jar are matched
        assertExcludes(manifest, LIB, "lucene-core-9_12_3.jar");
        assertNotExcludes(manifest, LIB, "lucene-core-9.12.3xjar");
    }

    @Test
    public void testOnlyJarsAreMatched()
    {
        // a pattern ending in .jar can still match other names through a top-level alternation
        ExecutorClasspathManifest manifest = parse("[lib-exclude]", "a|b\\.jar", ".*|x\\.jar");
        assertExcludes(manifest, LIB, "b.jar", "presto-spark-0.301.jar");
        assertNotExcludes(manifest, LIB, "a", "notes.txt", "launcher.properties");
    }

    @Test
    public void testMissingOrEmptySectionExcludesNothing()
    {
        ExecutorClasspathManifest pluginOnly = parse("[plugin-exclude]", "a.jar");
        assertNotExcludes(pluginOnly, LIB, "a.jar");
        assertExcludes(pluginOnly, PLUGIN, "a.jar");

        ExecutorClasspathManifest empty = parse("[lib-exclude]", "[plugin-exclude]");
        assertNotExcludes(empty, LIB, "a.jar");
        assertNotExcludes(empty, PLUGIN, "a.jar");
        assertFalse(empty.isRestricted());

        assertFalse(parse("# only comments", "").isRestricted());
    }

    @Test
    public void testRepeatedSectionAccumulates()
    {
        ExecutorClasspathManifest manifest = parse("[lib-exclude]", "a.jar", "[plugin-exclude]", "b.jar", "[lib-exclude]", "c.jar");
        assertExcludes(manifest, LIB, "a.jar", "c.jar");
        assertNotExcludes(manifest, LIB, "b.jar");
        assertExcludes(manifest, PLUGIN, "b.jar");
        assertNotExcludes(manifest, PLUGIN, "a.jar", "c.jar");
    }

    @Test
    public void testMalformed()
    {
        assertMalformed("line 1: a.jar appears before any section", "a.jar", "[lib-exclude]");
        assertMalformed("line 2: unknown section [jars]", "[lib-exclude]", "[jars]");
        // the old keep-list headers are not accepted: an old manifest fails open instead of being misread
        assertMalformed("line 1: unknown section [lib]", "[lib]", "a.jar");
        assertMalformed("line 1: expected [section] or a .jar file name pattern: [plugin-exclude", "[plugin-exclude", "a.jar");
        assertMalformed("line 2: expected [section] or a .jar file name pattern: notes.txt", "[lib-exclude]", "notes.txt");
        assertMalformed("line 2: expected [section] or a .jar file name pattern: a.jar b.jar", "[lib-exclude]", "a.jar b.jar");
        assertMalformed("line 1: unknown section [lib-excluded]", "[lib-excluded]", "a.jar");
        assertMalformed("line 1: expected [section] or a .jar file name pattern: [LIB-EXCLUDE]", "[LIB-EXCLUDE]", "a.jar");
        // patterns are matched against file names, so a path could never match
        assertMalformed("line 2: expected [section] or a .jar file name pattern: lib/a.jar", "[lib-exclude]", "lib/a.jar");
        // a pattern must end in .jar, so it cannot reach a non-jar file
        assertMalformed("line 2: expected [section] or a .jar file name pattern: spark-core-.*", "[lib-exclude]", "spark-core-.*");
        assertMalformed("line 2: invalid pattern a(.jar: Unclosed group", "[lib-exclude]", "a(.jar");
        assertMalformed("line 2: expected [section] or a .jar file name pattern: .jar", "[lib-exclude]", ".jar");
        // matching is always whole-name, so a trailing anchor is rejected rather than silently allowed
        assertMalformed("line 2: expected [section] or a .jar file name pattern: a\\.jar$", "[lib-exclude]", "a\\.jar$");
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
            assertTrue(malformedErr.contains("line 3: expected [section] or a .jar file name pattern: notes.txt"), malformedErr);

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
            assertNotExcludes(fallback, LIB, "a.jar");
            assertNotExcludes(fallback, PLUGIN, "a.jar");
            assertFalse(fallback.serviceConfig(ImmutableMap.of(), directory.toFile()).containsKey(PLUGIN_EXCLUDED_JARS));

            // a valid file loads
            Files.write(valid, ImmutableList.of("[lib-exclude]", "a.jar"), UTF_8);
            assertExcludes(ExecutorClasspathManifest.load(Optional.of(valid.toFile())), LIB, "a.jar");
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
        Map<String, String> direct = ExecutorClasspathManifest.UNRESTRICTED.serviceConfig(ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "a.jar"), new File("/nonexistent"));
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
            throws IOException
    {
        Path plugins = Files.createTempDirectory("plugins");
        Path hive = Files.createDirectory(plugins.resolve("hive"));
        List<Path> files = ImmutableList.of(Files.createFile(hive.resolve("a.jar")), Files.createFile(hive.resolve("presto-hive.jar")));
        try {
            File dir = plugins.toFile();
            Map<String, String> launcherConfig = ImmutableMap.of(CONFIG_PROPERTY, "executor-classpath.txt", "query.max-memory", "1GB");

            // the launcher's own key never reaches Presto, which would reject it as unused
            assertEquals(ExecutorClasspathManifest.UNRESTRICTED.serviceConfig(launcherConfig, dir), ImmutableMap.of("query.max-memory", "1GB"));

            Map<String, String> config = parse("[plugin-exclude]", "a.jar").serviceConfig(launcherConfig, dir);
            assertEquals(config, ImmutableMap.of("query.max-memory", "1GB", PLUGIN_EXCLUDED_JARS, "a.jar"));

            // a lib-only manifest leaves plugin loading alone
            assertEquals(parse("[lib-exclude]", "a.jar").serviceConfig(launcherConfig, dir), ImmutableMap.of("query.max-memory", "1GB"));

            // the exclude list replaces a value already in the config
            Map<String, String> withExisting = ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "stale.jar");
            assertEquals(parse("[plugin-exclude]", "a.jar").serviceConfig(withExisting, dir), ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "a.jar"));
        }
        finally {
            for (Path file : files) {
                Files.delete(file);
            }
            Files.delete(hive);
            Files.delete(plugins);
        }
    }

    @Test
    public void testPluginPatternsResolveToFileNames()
            throws IOException
    {
        Path plugins = Files.createTempDirectory("plugins");
        Path hive = Files.createDirectory(plugins.resolve("hive"));
        Path iceberg = Files.createDirectory(plugins.resolve("iceberg"));
        List<Path> files = ImmutableList.of(
                Files.createFile(hive.resolve("presto-hive.jar")),
                Files.createFile(hive.resolve("fastutil-8.5.2.jar")),
                Files.createFile(hive.resolve("hudi-presto-bundle-0.14.0.jar")),
                Files.createFile(iceberg.resolve("presto-iceberg.jar")),
                Files.createFile(iceberg.resolve("fastutil-8.5.9.jar")),
                Files.createFile(iceberg.resolve("fastutil-core-8.5.9.jar")),
                Files.createFile(iceberg.resolve("odd,name-1.0.jar")),
                Files.createFile(plugins.resolve("fastutil-9.0.0.jar")));
        try {
            File dir = plugins.toFile();
            ExecutorClasspathManifest manifest = parse("[plugin-exclude]", "fastutil-[0-9].*\\.jar", "odd,name-.*\\.jar", "absent-[0-9].*\\.jar");
            // every version a pattern matches, in any plugin directory; plugin.excluded-jars takes exact names.
            // Not fastutil-core (a different artifact), not a jar outside a plugin directory, and not a name
            // with a comma, which plugin.excluded-jars could not carry.
            assertEquals(manifest.serviceConfig(ImmutableMap.of(), dir), ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "fastutil-8.5.2.jar,fastutil-8.5.9.jar"));

            // patterns that match nothing still replace a value set directly
            assertEquals(parse("[plugin-exclude]", "absent-[0-9].*\\.jar").serviceConfig(ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "stale.jar"), dir),
                    ImmutableMap.of(PLUGIN_EXCLUDED_JARS, ""));

            // plugin directories that cannot be listed: fail open, saying so on stderr, and replace a
            // value set directly just as when the patterns match nothing
            File missing = new File(dir, "missing");
            String err = stderrOf(() -> assertEquals(manifest.serviceConfig(ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "stale.jar"), missing),
                    ImmutableMap.of(PLUGIN_EXCLUDED_JARS, "")));
            assertTrue(err.contains("could not apply the executor classpath manifest's [plugin-exclude] to " + missing), err);
            assertTrue(err.contains("excluding no plugin jars"), err);
        }
        finally {
            for (Path file : files) {
                Files.delete(file);
            }
            Files.delete(hive);
            Files.delete(iceberg);
            Files.delete(plugins);
        }
    }

    private static Optional<File> configuredFile(SparkProcessType processType, Map<String, String> config, boolean nativeWorkerConfigured)
    {
        return ExecutorClasspathManifest.configuredFile(processType, config, nativeWorkerConfigured, "/pkg");
    }

    private static ExecutorClasspathManifest parse(String... lines)
    {
        return ExecutorClasspathManifest.parse(ImmutableList.copyOf(lines));
    }

    private static void assertExcludes(ExecutorClasspathManifest manifest, ExecutorClasspathManifest.Section section, String... fileNames)
    {
        for (String fileName : fileNames) {
            assertTrue(manifest.excludes(section, fileName), section + " should exclude " + fileName);
        }
    }

    private static void assertNotExcludes(ExecutorClasspathManifest manifest, ExecutorClasspathManifest.Section section, String... fileNames)
    {
        for (String fileName : fileNames) {
            assertFalse(manifest.excludes(section, fileName), section + " should not exclude " + fileName);
        }
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
