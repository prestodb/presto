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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.facebook.presto.spark.launcher.LauncherUtils.listFiles;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllLines;
import static java.util.Objects.requireNonNull;

/**
 * The jars an executor must not open, per classpath, as listed in an executor classpath manifest.
 *
 * <p>Format: a {@code [lib-exclude]} section and a {@code [plugin-exclude]} section, one Java
 * regular expression per line, matched against the whole jar file name and ending in {@code .jar}
 * (normally {@code \.jar}). Only files whose name ends in {@code .jar} are ever matched, whatever
 * the pattern. {@code #} starts a comment anywhere on a line. A pattern is matched against file
 * names only, so {@code [plugin-exclude]} patterns apply in every plugin directory.
 * Patterns let an entry survive a version bump: {@code spark-core-[0-9].*\.jar} keeps matching
 * when {@code spark-core-3.4.1-1.jar} becomes {@code spark-core-3.4.2-1.jar}. A plain file name
 * is also a valid pattern: it matches that name, though its unescaped dots match any character.
 * Every jar no pattern matches stays on the classpath,
 * including jars added to the package after the list was written; a pattern that matches nothing
 * is simply ignored. A missing or empty section excludes nothing.
 *
 * <pre>
 * # comment
 * [lib-exclude]
 * spark-core-[0-9].*\.jar
 * [plugin-exclude]
 * hudi-presto-bundle-[0-9].*\.jar
 * </pre>
 *
 * <p>An exclude list rather than a keep list, on purpose: the memory is concentrated in a few
 * large jars (on a native executor the 20 largest unused lib/ jars hold 92% of what the full
 * unused set holds), so excluding just those keeps most of the saving while confining the risk
 * of a missing class to jars that were each checked, instead of every jar a keep list leaves out.
 */
final class ExecutorClasspathManifest
{
    /**
     * Presto config property naming the manifest. A relative path is resolved against the
     * package directory.
     *
     * <p><b>Unset by default, and unset means no change:</b> every executor keeps the full
     * classpath, exactly like the driver.
     *
     * <p>Why narrow it at all: the JDK keeps an on-heap copy of the central directory of every
     * <em>opened</em> jar ({@code ZipFile$Source}) for the life of the process, and any lookup
     * that misses walks the whole classpath and opens every jar on it. An executor on the native
     * path is a coordination shim that hands plan fragments to the native worker, so a handful of
     * large jars it never uses cost it tens of MiB of heap.
     */
    static final String CONFIG_PROPERTY = "presto.spark.executor-classpath-manifest";

    /** PluginManagerConfig.PLUGIN_EXCLUDED_JARS, as a literal: the launcher must not depend on presto-main-base. */
    static final String PLUGIN_EXCLUDED_JARS = "plugin.excluded-jars";

    /** FeaturesConfig's @Config("native-execution-enabled"). Read as a literal: the launcher
     *  runs in Spark's classloader and must not depend on presto-main-base. */
    private static final String NATIVE_EXECUTION_ENABLED = "native-execution-enabled";

    /** The sections of the manifest. */
    enum Section
    {
        LIB("lib-exclude"),
        PLUGIN("plugin-exclude");

        private final String header;

        Section(String header)
        {
            this.header = header;
        }
    }

    /** Nothing excluded anywhere: what every process gets without a usable manifest. */
    static final ExecutorClasspathManifest UNRESTRICTED = new ExecutorClasspathManifest(new EnumMap<>(Section.class));

    /**
     * One manifest line: a section header ({@code [lib-exclude]}), a jar file name pattern, or
     * nothing -- each optionally followed by a comment. A pattern has no whitespace, {@code #} or
     * {@code /} (it is matched against file names, never paths) and ends in {@code .jar}, which
     * also keeps it apart from a section header. Anchors are not needed, and a trailing {@code $}
     * is rejected: a pattern always has to match the whole name.
     */
    private static final Pattern LINE = Pattern.compile(
            "\\s*(?:\\[(?<section>[a-z-]+)]|(?<pattern>[^\\s#/]+\\.jar))?\\s*(?:#.*)?");

    private final Map<Section, List<Pattern>> sections;

    private ExecutorClasspathManifest(Map<Section, List<Pattern>> sections)
    {
        this.sections = requireNonNull(sections, "sections is null");
    }

    /**
     * The manifest a process should apply, if any. Four conditions, all required.
     *
     * <p>The manifest must be CONFIGURED. Without {@link #CONFIG_PROPERTY} nothing changes, so
     * this is opt-in per deployment.
     *
     * <p>The process must be a real EXECUTOR. The driver plans queries and reaches function
     * namespace managers and the JDBC/JDBI stack reflectively -- paths no executor sampling can
     * see, and what broke two earlier attempts to drop jars from the package outright. A local
     * executor is excluded too: it shares the driver's JVM, and nothing was sampled in local
     * mode.
     *
     * <p>NATIVE EXECUTION is required because that is the only configuration an exclude list can
     * be sampled from safely. A native executor loads ~198 classes; a JAVA-path executor runs
     * LocalExecutionPlanner, the operator factories, the codegen compilers, the spillers and
     * the Java ORC/Parquet/DWRF readers -- roughly 3,400 classes, from jars a native sample
     * never sees. A query that switches to the Java engine through the session property is
     * refused by the launcher instead (see PrestoSparkRunner).
     *
     * <p>A NATIVE WORKER must also be configured. Together with native-execution-enabled this
     * identifies a process whose real work runs in a native (Velox) worker, rather than one
     * where the flag is set but no worker exists to hand fragments to.
     */
    static Optional<File> configuredFile(
            SparkProcessType sparkProcessType,
            Map<String, String> configProperties,
            boolean nativeWorkerConfigured,
            String packagePath)
    {
        String configured = configProperties.getOrDefault(CONFIG_PROPERTY, "").trim();
        if (configured.isEmpty()
                || sparkProcessType != SparkProcessType.EXECUTOR
                || !Boolean.parseBoolean(configProperties.getOrDefault(NATIVE_EXECUTION_ENABLED, "false"))
                || !nativeWorkerConfigured) {
            return Optional.empty();
        }
        File manifest = new File(configured);
        return Optional.of(manifest.isAbsolute() ? manifest : new File(packagePath, configured));
    }

    /**
     * Refuses the Java engine on an executor whose classpath was actually narrowed. The engine is
     * chosen per query (the native_execution_enabled session property), after the executor's
     * classpath was fixed at startup, and a narrowed classpath may lack jars the Java engine
     * loads. Failing here names the cause; otherwise the task would fail later with a
     * NoClassDefFoundError deep inside the Java engine.
     *
     * @param classpathNarrowed whether any jar was actually left off this executor's lib/ or
     *         plugin classpath -- by this manifest or by plugin.excluded-jars set directly -- after
     *         every fallback that restores the full classpath
     */
    static void checkJavaEngineAllowed(boolean classpathNarrowed)
    {
        if (classpathNarrowed) {
            throw new IllegalStateException(format(
                    "this executor's classpath was narrowed (by %s or %s), but the query runs on the Java engine "
                            + "(native_execution_enabled=false); keep native execution enabled for this query, or remove those settings",
                    CONFIG_PROPERTY,
                    PLUGIN_EXCLUDED_JARS));
        }
    }

    /** The plugin jar names excluded by a Presto config: its {@code plugin.excluded-jars}, if any. */
    static Set<String> pluginExclusions(Map<String, String> config)
    {
        Set<String> jars = new HashSet<>();
        for (String name : config.getOrDefault(PLUGIN_EXCLUDED_JARS, "").split(",")) {
            if (!name.trim().isEmpty()) {
                jars.add(name.trim());
            }
        }
        return jars;
    }

    /**
     * Whether excluding {@code excludedJars} actually leaves a jar out of some plugin directory.
     * Mirrors PluginManagerUtil.selectPluginJars, which the launcher cannot depend on: a
     * directory loses a jar only if it contains one of the excluded names and still keeps at
     * least one jar; a directory that would be left with no jars is loaded whole. If the plugin
     * directories cannot be read, any exclusion counts as applied, which errs toward refusing the
     * Java engine rather than letting a query hit a missing class.
     */
    static boolean pluginExclusionsApplied(File pluginsDirectory, Set<String> excludedJars)
    {
        if (excludedJars.isEmpty()) {
            return false;
        }
        try {
            for (File directory : listFiles(pluginsDirectory)) {
                if (!directory.isDirectory()) {
                    continue;
                }
                int jars = 0;
                int excluded = 0;
                for (File file : listFiles(directory)) {
                    if (file.getName().endsWith(".jar")) {
                        jars++;
                        if (excludedJars.contains(file.getName())) {
                            excluded++;
                        }
                    }
                }
                if (excluded > 0 && excluded < jars) {
                    return true;
                }
            }
            return false;
        }
        catch (Exception e) {
            return true;
        }
    }

    /** Whether anything is excluded. */
    boolean isRestricted()
    {
        return !patterns(Section.LIB).isEmpty() || !patterns(Section.PLUGIN).isEmpty();
    }

    /**
     * The Presto config to start a service with, given the launcher's config properties. A copy,
     * so the caller's map -- which an executor compares across tasks -- is left as it was.
     *
     * <p>{@link #CONFIG_PROPERTY} is removed for every process: it is the launcher's setting, no
     * Presto config class binds it, and airlift's strict config fails startup on an unused
     * property. If there is a {@code [plugin-exclude]} section, the jar names its patterns match
     * in {@code pluginsDirectory} are added as {@code plugin.excluded-jars}, replacing a value
     * already in the config -- also when they match nothing, or the plugin directories cannot be
     * listed: the plugin manager takes exact names, not patterns.
     */
    Map<String, String> serviceConfig(Map<String, String> configProperties, File pluginsDirectory)
    {
        Map<String, String> config = new HashMap<>(configProperties);
        config.remove(CONFIG_PROPERTY);
        if (!patterns(Section.PLUGIN).isEmpty()) {
            config.put(PLUGIN_EXCLUDED_JARS, String.join(",", matchingPluginJars(pluginsDirectory)));
        }
        return config;
    }

    /**
     * The jar file names in any plugin directory that {@code [plugin-exclude]} matches, or none
     * if the plugin directories cannot be listed -- failing open, with the reason on stderr, like
     * {@link #load}. A name containing {@code ,} is skipped: plugin.excluded-jars is
     * comma-separated, so it could not be passed on.
     */
    private Set<String> matchingPluginJars(File pluginsDirectory)
    {
        Set<String> jars = new TreeSet<>();
        try {
            for (File directory : listFiles(pluginsDirectory)) {
                if (!directory.isDirectory()) {
                    continue;
                }
                for (File file : listFiles(directory)) {
                    String name = file.getName();
                    if (!name.contains(",") && excludes(Section.PLUGIN, name)) {
                        jars.add(name);
                    }
                }
            }
        }
        catch (Exception e) {
            System.err.println("[presto-spark] could not apply the executor classpath manifest's [plugin-exclude] to "
                    + pluginsDirectory + "; excluding no plugin jars. Cause: " + e);
            return Collections.emptySet();
        }
        return jars;
    }

    /**
     * Whether {@code fileName} is a jar and a pattern in {@code section} matches the whole of it.
     * The jar check is not implied by patterns ending in {@code .jar}: {@code a|b.jar} matches
     * {@code a}.
     */
    boolean excludes(Section section, String fileName)
    {
        if (!fileName.endsWith(".jar")) {
            return false;
        }
        for (Pattern pattern : patterns(section)) {
            if (pattern.matcher(fileName).matches()) {
                return true;
            }
        }
        return false;
    }

    private List<Pattern> patterns(Section section)
    {
        return Collections.unmodifiableList(sections.getOrDefault(section, Collections.emptyList()));
    }

    /**
     * Reads the manifest, or {@link #UNRESTRICTED} if there is none. Fails open: an unreadable
     * or malformed file also means {@link #UNRESTRICTED}, with the reason on stderr. A classpath
     * this list gets wrong is a startup crash on every task, whereas the cost of ignoring it is
     * only the memory it would have saved.
     */
    static ExecutorClasspathManifest load(Optional<File> manifest)
    {
        if (!manifest.isPresent()) {
            return UNRESTRICTED;
        }
        // Everything is inside the try: toPath() and readAllLines() throw InvalidPathException,
        // SecurityException and IOException, parsing throws IllegalArgumentException, and any
        // escape would kill the executor at startup rather than fall back.
        try {
            return parse(readAllLines(manifest.get().toPath(), UTF_8));
        }
        catch (Exception e) {
            // Say so on stderr. Failing open is safe but SILENT, and a silent fallback is
            // indistinguishable from the optimisation working -- the executor starts, the
            // query succeeds, and the only symptom is memory that never dropped. There is no
            // logger here: the launcher runs in Spark's classloader before Presto logging
            // exists, so stderr is what reaches the executor log.
            System.err.println("[presto-spark] could not use executor classpath manifest " + manifest.get()
                    + "; using the full classpath. Cause: " + e);
            return UNRESTRICTED;
        }
    }

    /**
     * Parses manifest lines. Throws IllegalArgumentException, naming the line, on any line
     * {@link #LINE} does not accept, on a pattern that does not compile, on an unknown section,
     * and on an entry before the first section. A section that appears twice accumulates.
     */
    static ExecutorClasspathManifest parse(List<String> lines)
    {
        Map<Section, List<Pattern>> sections = new EnumMap<>(Section.class);
        List<Pattern> current = null;
        for (int i = 0; i < lines.size(); i++) {
            Matcher line = LINE.matcher(lines.get(i));
            if (!line.matches()) {
                throw new IllegalArgumentException(format("line %s: expected [section] or a .jar file name pattern: %s", i + 1, lines.get(i)));
            }
            if (line.group("section") != null) {
                current = sections.computeIfAbsent(section(line.group("section"), i + 1), section -> new ArrayList<>());
            }
            else if (line.group("pattern") != null) {
                if (current == null) {
                    throw new IllegalArgumentException(format("line %s: %s appears before any section", i + 1, line.group("pattern")));
                }
                try {
                    current.add(Pattern.compile(line.group("pattern")));
                }
                catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException(format("line %s: invalid pattern %s: %s", i + 1, line.group("pattern"), e.getDescription()));
                }
            }
        }
        return new ExecutorClasspathManifest(sections);
    }

    private static Section section(String header, int lineNumber)
    {
        for (Section section : Section.values()) {
            if (section.header.equals(header)) {
                return section;
            }
        }
        throw new IllegalArgumentException(format("line %s: unknown section [%s]", lineNumber, header));
    }
}
