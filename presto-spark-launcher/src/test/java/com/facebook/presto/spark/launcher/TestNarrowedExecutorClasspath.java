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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.classloader_interface.SparkProcessType.DRIVER;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.EXECUTOR;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.LOCAL_EXECUTOR;
import static com.facebook.presto.spark.launcher.NarrowedExecutorClasspath.CONFIG_PROPERTY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

// single-threaded: the tests share the temporary package directory
@Test(singleThreaded = true)
public class TestNarrowedExecutorClasspath
{
    private Path packageDirectory;
    private Path lib;
    private Path plugins;
    private Path libNative;
    private Path pluginNative;

    @BeforeMethod
    public void setUp()
            throws IOException
    {
        // <package>/
        //   lib/            presto-spark.jar, spark-core-3.4.1.jar
        //   plugin/hive/    presto-hive.jar, guava.jar
        //   plugin/tpch/    presto-tpch.jar
        //   lib-native/     presto-spark.jar -> ../lib/presto-spark.jar
        //   plugin-native/  hive/presto-hive.jar -> ../../plugin/hive/presto-hive.jar, tpch -> ../plugin/tpch
        packageDirectory = Files.createTempDirectory("package");
        lib = Files.createDirectory(packageDirectory.resolve("lib"));
        Files.createFile(lib.resolve("presto-spark.jar"));
        Files.createFile(lib.resolve("spark-core-3.4.1.jar"));
        plugins = Files.createDirectory(packageDirectory.resolve("plugin"));
        Files.createFile(Files.createDirectory(plugins.resolve("hive")).resolve("presto-hive.jar"));
        Files.createFile(plugins.resolve("hive/guava.jar"));
        Files.createFile(Files.createDirectory(plugins.resolve("tpch")).resolve("presto-tpch.jar"));
        libNative = Files.createDirectory(packageDirectory.resolve("lib-native"));
        Files.createSymbolicLink(libNative.resolve("presto-spark.jar"), Paths.get("../lib/presto-spark.jar"));
        pluginNative = Files.createDirectory(packageDirectory.resolve("plugin-native"));
        Files.createSymbolicLink(Files.createDirectory(pluginNative.resolve("hive")).resolve("presto-hive.jar"), Paths.get("../../plugin/hive/presto-hive.jar"));
        Files.createSymbolicLink(pluginNative.resolve("tpch"), Paths.get("../plugin/tpch"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        MoreFiles.deleteRecursively(packageDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testIsEnabled()
    {
        Map<String, String> config = ImmutableMap.of(CONFIG_PROPERTY, "true", "native-execution-enabled", "true");
        assertTrue(NarrowedExecutorClasspath.isEnabled(EXECUTOR, config, true));
        assertTrue(NarrowedExecutorClasspath.isEnabled(EXECUTOR, ImmutableMap.of(CONFIG_PROPERTY, " true ", "native-execution-enabled", "true"), true));

        assertFalse(NarrowedExecutorClasspath.isEnabled(EXECUTOR, ImmutableMap.of("native-execution-enabled", "true"), true));
        assertFalse(NarrowedExecutorClasspath.isEnabled(EXECUTOR, ImmutableMap.of(CONFIG_PROPERTY, "false", "native-execution-enabled", "true"), true));
        assertFalse(NarrowedExecutorClasspath.isEnabled(DRIVER, config, true));
        assertFalse(NarrowedExecutorClasspath.isEnabled(LOCAL_EXECUTOR, config, true));
        assertFalse(NarrowedExecutorClasspath.isEnabled(EXECUTOR, ImmutableMap.of(CONFIG_PROPERTY, "true"), true));
        assertFalse(NarrowedExecutorClasspath.isEnabled(EXECUTOR, config, false));
    }

    @Test
    public void testServiceConfig()
    {
        Map<String, String> launcherConfig = ImmutableMap.of(CONFIG_PROPERTY, "true", "query.max-memory", "1GB");
        // the launcher's own key never reaches Presto, which would reject it as unused
        assertEquals(NarrowedExecutorClasspath.serviceConfig(launcherConfig), ImmutableMap.of("query.max-memory", "1GB"));
        // the caller's map is left as it was
        assertEquals(launcherConfig.get(CONFIG_PROPERTY), "true");
    }

    @Test
    public void testJavaEngineRefusedOnNarrowedExecutor()
    {
        IllegalStateException refused = expectThrows(IllegalStateException.class, () -> NarrowedExecutorClasspath.checkJavaEngineAllowed(true));
        assertEquals(refused.getMessage(),
                "this executor's classpath was narrowed (by presto.spark.narrowed-executor-classpath-enabled), "
                        + "but the query runs on the Java engine (native_execution_enabled=false); "
                        + "keep native execution enabled for this query, or remove that setting");

        // nothing actually narrowed: the Java engine is allowed
        NarrowedExecutorClasspath.checkJavaEngineAllowed(false);
    }

    @Test
    public void testNarrowed()
    {
        String[] err = new String[1];
        assertEquals(narrowed(lib, err), Optional.of(libNative.toFile()));
        assertTrue(err[0].contains("[presto-spark] using " + libNative + " instead of " + lib + " (1 links, checked in "), err[0]);

        assertEquals(narrowed(plugins, err), Optional.of(pluginNative.toFile()));
        assertTrue(err[0].contains("(2 links, checked in "), err[0]);
    }

    @Test
    public void testNarrowedNotUsable()
            throws IOException
    {
        // none in the package: nothing said
        MoreFiles.deleteRecursively(libNative, RecursiveDeleteOption.ALLOW_INSECURE);
        String[] err = new String[1];
        assertEquals(narrowed(lib, err), Optional.empty());
        assertEquals(err[0], "");

        // a link extracted as a plain file
        Path keptLink = pluginNative.resolve("hive/presto-hive.jar");
        Files.delete(keptLink);
        Files.createFile(keptLink);
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertTrue(err[0].contains("[presto-spark] not using " + pluginNative + ": " + keptLink + " is not a link; using " + plugins), err[0]);

        // a link that no longer resolves: the class loader would skip the jar without an error
        relink(keptLink, "../../plugin/hive/gone.jar");
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertTrue(err[0].contains(keptLink + " does not resolve"), err[0]);

        // a plugin directory linked whole that no longer resolves: the plugin manager would skip it
        relink(keptLink, "../../plugin/hive/presto-hive.jar");
        assertTrue(narrowed(plugins, err).isPresent());
        relink(pluginNative.resolve("tpch"), "../plugin/missing");
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertTrue(err[0].contains(pluginNative.resolve("tpch") + " does not resolve"), err[0]);

        // lib-native itself a link (to a copy, say)
        Files.createSymbolicLink(libNative, lib);
        assertEquals(narrowed(lib, err), Optional.empty());
        assertTrue(err[0].contains("not using " + libNative + ": " + libNative + " is not a directory"), err[0]);
    }

    @Test
    public void testEmptyCopyIsIgnored()
            throws IOException
    {
        // a package build may create the copies empty when it leaves nothing out
        MoreFiles.deleteDirectoryContents(libNative, RecursiveDeleteOption.ALLOW_INSECURE);
        MoreFiles.deleteDirectoryContents(pluginNative, RecursiveDeleteOption.ALLOW_INSECURE);
        String[] err = new String[1];
        assertEquals(narrowed(lib, err), Optional.empty());
        assertEquals(err[0], "");
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertEquals(err[0], "");
    }

    @Test
    public void testPluginCopyNeedsEveryPlugin()
            throws IOException
    {
        // a plugin missing from the copy would be skipped silently
        Files.delete(pluginNative.resolve("tpch"));
        String[] err = new String[1];
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertTrue(err[0].contains("not using " + pluginNative + ": " + pluginNative + " lacks [tpch] and has [], compared with " + plugins), err[0]);

        // an entry plugin/ does not have
        Files.createSymbolicLink(pluginNative.resolve("tpch"), Paths.get("../plugin/tpch"));
        Files.createSymbolicLink(pluginNative.resolve("extra"), Paths.get("../plugin/tpch"));
        assertEquals(narrowed(plugins, err), Optional.empty());
        assertTrue(err[0].contains("lacks [] and has [extra]"), err[0]);

        // lib-native is not compared: leaving files out is its purpose
        assertTrue(narrowed(lib, err).isPresent());
    }

    private static void relink(Path link, String target)
            throws IOException
    {
        Files.delete(link);
        Files.createSymbolicLink(link, Paths.get(target));
    }

    private static Optional<File> narrowed(Path directory, String[] err)
    {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(captured, true);
        Optional<File> narrowed = directory.getFileName().toString().equals("plugin")
                ? NarrowedExecutorClasspath.narrowedPlugins(directory.toFile(), out)
                : NarrowedExecutorClasspath.narrowedLib(directory.toFile(), out);
        err[0] = captured.toString();
        return narrowed;
    }
}
