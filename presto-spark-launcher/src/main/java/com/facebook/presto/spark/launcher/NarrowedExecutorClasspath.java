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
import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spark.launcher.LauncherUtils.listFiles;
import static java.lang.String.format;

/**
 * Narrowed copies of the package's {@code lib/} and {@code plugin/} for native executors:
 * {@value #LIB_DIRECTORY} and {@value #PLUGIN_DIRECTORY}, directories of symbolic links without the
 * jars a native executor never uses. Each opened jar keeps its central directory on the heap
 * ({@code ZipFile$Source}), so unused large jars cost tens of MiB.
 *
 * <p>The package build creates the copies, next to the originals:
 * <ul>
 * <li>{@value #LIB_DIRECTORY}: a link per {@code lib/} file kept.</li>
 * <li>{@value #PLUGIN_DIRECTORY}: an entry per {@code plugin/} entry, by the same name: a link to
 *     the original, or a directory of links to the files kept. A plugin that would lose every jar
 *     must be linked whole.</li>
 * <li>Links should be relative and must resolve. A missing or empty copy leaves that directory
 *     whole.</li>
 * </ul>
 */
final class NarrowedExecutorClasspath
{
    /** Presto config property; false (the default) keeps the full classpath everywhere. */
    static final String CONFIG_PROPERTY = "presto.spark.narrowed-executor-classpath-enabled";

    static final String LIB_DIRECTORY = "lib-native";
    static final String PLUGIN_DIRECTORY = "plugin-native";

    // FeaturesConfig's key; the launcher must not depend on presto-main-base
    private static final String NATIVE_EXECUTION_ENABLED = "native-execution-enabled";

    private NarrowedExecutorClasspath() {}

    /**
     * Whether to load the copies: only when enabled, on a cluster executor (never the driver or
     * local mode, whose class use was not sampled), with native execution and a native worker.
     */
    static boolean isEnabled(SparkProcessType sparkProcessType, Map<String, String> configProperties, boolean nativeWorkerConfigured)
    {
        return Boolean.parseBoolean(configProperties.getOrDefault(CONFIG_PROPERTY, "false").trim())
                && sparkProcessType == SparkProcessType.EXECUTOR
                && Boolean.parseBoolean(configProperties.getOrDefault(NATIVE_EXECUTION_ENABLED, "false"))
                && nativeWorkerConfigured;
    }

    /**
     * A copy of the config without {@link #CONFIG_PROPERTY}, which strict config would reject as
     * unused. The caller's map is left as it was.
     */
    static Map<String, String> serviceConfig(Map<String, String> configProperties)
    {
        Map<String, String> config = new HashMap<>(configProperties);
        config.remove(CONFIG_PROPERTY);
        return config;
    }

    /**
     * Refuses a Java-engine query on a narrowed executor, which may lack jars the Java engine
     * needs, instead of failing later with a NoClassDefFoundError.
     */
    static void checkJavaEngineAllowed(boolean classpathNarrowed)
    {
        if (classpathNarrowed) {
            throw new IllegalStateException(format(
                    "this executor's classpath was narrowed (by %s), but the query runs on the Java engine "
                            + "(native_execution_enabled=false); keep native execution enabled for this query, or remove that setting",
                    CONFIG_PROPERTY));
        }
    }

    /** {@value #LIB_DIRECTORY}, if usable (see {@link #narrowed}). */
    static Optional<File> narrowedLib(File lib, PrintStream err)
    {
        return narrowed(lib, false, err);
    }

    /**
     * {@value #PLUGIN_DIRECTORY}, if usable (see {@link #narrowed}) and it has the same entries as
     * {@code plugins}: a missing plugin would otherwise be skipped silently.
     */
    static Optional<File> narrowedPlugins(File plugins, PrintStream err)
    {
        return narrowed(plugins, true, err);
    }

    /**
     * {@code <directory>-native}, if it is a directory of links that resolve and of directories of
     * such links. Empty, silently, if it is missing or empty; empty, with the reason on
     * {@code err}, if it is unusable: plain files (from an archive that dropped the links) or
     * dangling links would fail silently later.
     */
    private static Optional<File> narrowed(File directory, boolean sameEntries, PrintStream err)
    {
        File narrowed = narrowedPath(directory).toFile();
        if (!Files.exists(narrowed.toPath(), LinkOption.NOFOLLOW_LINKS)) {
            return Optional.empty();
        }
        long start = System.nanoTime();
        try {
            if (!Files.isDirectory(narrowed.toPath(), LinkOption.NOFOLLOW_LINKS)) {
                return reject(narrowed, narrowed + " is not a directory", directory, err);
            }
            if (listFiles(narrowed).length == 0) {
                return Optional.empty();
            }
            LinkCount links = new LinkCount();
            Optional<String> problem = firstProblem(narrowed.toPath(), links);
            if (!problem.isPresent() && sameEntries) {
                problem = entriesMismatch(narrowed, directory);
            }
            if (problem.isPresent()) {
                return reject(narrowed, problem.get(), directory, err);
            }
            // logged on success too, so a silent fallback is visible
            err.println(format("[presto-spark] using %s instead of %s (%s links, checked in %.1f ms)",
                    narrowed, directory, links.count, (System.nanoTime() - start) / 1e6));
            return Optional.of(narrowed);
        }
        catch (Exception e) {
            err.println("[presto-spark] not using " + narrowed + "; using " + directory + ". Cause: " + e);
            return Optional.empty();
        }
    }

    private static Optional<File> reject(File narrowed, String reason, File directory, PrintStream err)
    {
        err.println("[presto-spark] not using " + narrowed + ": " + reason + "; using " + directory);
        return Optional.empty();
    }

    /** The first entry that is neither a link that resolves nor a directory of such links. */
    private static Optional<String> firstProblem(Path directory, LinkCount links)
    {
        for (File file : listFiles(directory.toFile())) {
            Path path = file.toPath();
            if (Files.isSymbolicLink(path)) {
                if (!Files.exists(path)) {
                    return Optional.of(path + " does not resolve");
                }
                links.count++;
            }
            else if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                Optional<String> problem = firstProblem(path, links);
                if (problem.isPresent()) {
                    return problem;
                }
            }
            else {
                return Optional.of(path + " is not a link");
            }
        }
        return Optional.empty();
    }

    private static Optional<String> entriesMismatch(File narrowed, File original)
    {
        Set<String> copy = names(narrowed);
        Set<String> expected = names(original);
        if (copy.equals(expected)) {
            return Optional.empty();
        }
        return Optional.of(format("%s lacks %s and has %s, compared with %s",
                narrowed, Sets.difference(expected, copy), Sets.difference(copy, expected), original));
    }

    private static Set<String> names(File directory)
    {
        Set<String> names = new HashSet<>();
        for (File file : listFiles(directory)) {
            names.add(file.getName());
        }
        return names;
    }

    private static final class LinkCount
    {
        private int count;
    }

    private static Path narrowedPath(File directory)
    {
        Path absolute = directory.toPath().toAbsolutePath();
        return absolute.resolveSibling(absolute.getFileName() + "-native");
    }
}
