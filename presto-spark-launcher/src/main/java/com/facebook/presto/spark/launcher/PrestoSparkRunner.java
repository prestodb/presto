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

import com.facebook.presto.spark.classloader_interface.ExecutionStrategy;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkBootstrapTimer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFailure;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import org.apache.spark.TaskContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.launcher.LauncherUtils.checkDirectory;
import static com.facebook.presto.spark.launcher.LauncherUtils.listFiles;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Ticker.systemTicker;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRunner
        implements AutoCloseable
{
    private final PrestoSparkDistribution distribution;
    private final IPrestoSparkService driverPrestoSparkService;
    private static final CollectionAccumulator<Map<String, Long>> bootstrapMetricsCollector = new CollectionAccumulator<>();
    public static final String SPARK_EXECUTION_STRATEGIES = "spark_execution_strategies";

    public PrestoSparkRunner(PrestoSparkDistribution distribution)
    {
        this.distribution = requireNonNull(distribution, "distribution is null");
        bootstrapMetricsCollector.register(distribution.getSparkContext(), Option.apply("PrestoOnSparkBootstrapMetrics"), false);
        driverPrestoSparkService = createService(
                SparkProcessType.DRIVER,
                distribution.getPackageSupplier(),
                distribution.getConfigProperties(),
                distribution.getCatalogProperties(),
                distribution.getPrestoSparkProperties(),
                distribution.getNativeWorkerConfigProperties(),
                distribution.getNativeWorkerCatalogConfigProperties(),
                distribution.getEventListenerProperties(),
                distribution.getAccessControlProperties(),
                distribution.getSessionPropertyConfigurationProperties(),
                distribution.getFunctionNamespaceProperties(),
                distribution.getTempStorageProperties(),
                distribution.getExpressionManagerProperties(),
                Optional.empty(),
                false,
                () -> {}).service;
    }

    public void run(
            String user,
            Optional<Principal> principal,
            Map<String, String> extraCredentials,
            List<X509Certificate> certificates,
            String catalog,
            String schema,
            Optional<String> source,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Map<String, String> sessionProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Optional<String> sqlText,
            Optional<String> sqlLocation,
            Optional<String> sqlFileHexHash,
            Optional<String> sqlFileSizeInBytes,
            Optional<String> traceToken,
            Optional<String> sparkQueueName,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation)
    {
        IPrestoSparkQueryExecutionFactory queryExecutionFactory = driverPrestoSparkService.getQueryExecutionFactory();
        PrestoSparkRunnerContext prestoSparkRunnerContext = new PrestoSparkRunnerContext(
                user,
                principal,
                extraCredentials,
                certificates,
                catalog,
                schema,
                source,
                userAgent,
                clientInfo,
                clientTags,
                sessionProperties,
                catalogSessionProperties,
                sqlText,
                sqlLocation,
                sqlFileHexHash,
                sqlFileSizeInBytes,
                traceToken,
                sparkQueueName,
                queryStatusInfoOutputLocation,
                queryDataOutputLocation,
                getExecutionStrategies(sessionProperties));
        try {
            execute(queryExecutionFactory, prestoSparkRunnerContext);
        }
        catch (PrestoSparkFailure failure) {
            if (!failure.getRetryExecutionStrategies().isEmpty()) {
                PrestoSparkRunnerContext retryRunnerContext = new PrestoSparkRunnerContext.Builder(prestoSparkRunnerContext)
                        .setExecutionStrategies(failure.getRetryExecutionStrategies())
                        .build();
                execute(queryExecutionFactory, retryRunnerContext);
                return;
            }

            throw failure;
        }
    }

    private List<ExecutionStrategy> getExecutionStrategies(Map<String, String> sessionProperties)
    {
        String executionStrategies = sessionProperties.getOrDefault(SPARK_EXECUTION_STRATEGIES, "");
        return Splitter.on(',').trimResults().omitEmptyStrings().splitToList(executionStrategies)
                .stream()
                .map(t -> ExecutionStrategy.valueOf(t))
                .collect(Collectors.toList());
    }

    private void execute(IPrestoSparkQueryExecutionFactory queryExecutionFactory, PrestoSparkRunnerContext prestoSparkRunnerContext)
    {
        PrestoSparkSession session = new PrestoSparkSession(
                prestoSparkRunnerContext.getUser(),
                prestoSparkRunnerContext.getPrincipal(),
                prestoSparkRunnerContext.getExtraCredentials(),
                prestoSparkRunnerContext.getCertificates(),
                Optional.ofNullable(prestoSparkRunnerContext.getCatalog()),
                Optional.ofNullable(prestoSparkRunnerContext.getSchema()),
                prestoSparkRunnerContext.getSource(),
                prestoSparkRunnerContext.getUserAgent(),
                prestoSparkRunnerContext.getClientInfo(),
                prestoSparkRunnerContext.getClientTags(),
                Optional.empty(),
                Optional.empty(),
                prestoSparkRunnerContext.getSessionProperties(),
                prestoSparkRunnerContext.getCatalogSessionProperties(),
                prestoSparkRunnerContext.getTraceToken());

        IPrestoSparkQueryExecution queryExecution = queryExecutionFactory.create(
                distribution.getSparkContext(),
                session,
                prestoSparkRunnerContext.getSqlText(),
                prestoSparkRunnerContext.getSqlLocation(),
                prestoSparkRunnerContext.getSqlFileHexHash(),
                prestoSparkRunnerContext.getSqlFileSizeInBytes(),
                prestoSparkRunnerContext.getSparkQueueName(),
                new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(distribution, bootstrapMetricsCollector),
                prestoSparkRunnerContext.getQueryStatusInfoOutputLocation(),
                prestoSparkRunnerContext.getQueryDataOutputLocation(),
                prestoSparkRunnerContext.getExecutionStrategies(),
                Optional.of(bootstrapMetricsCollector));

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
    }

    @Override
    public void close()
    {
        // Shutdown the driver Airlift application
        driverPrestoSparkService.close();

        // If we are in localMode, the executor spawns the Executor Airlift application
        // (which is long-running and holds onto resources) on the same JVM.
        //
        // On query completion, the SparkContext shutdown calls the Driver Airlift
        // application shutdown, but it has no hook to call Executor Airlift application
        // shutdown. So the query hangs forever.
        //
        // This code, prevents this hanging state by explicitly calling the
        // Executor Airlift application shutdown.
        DistributionBasedPrestoSparkTaskExecutorFactoryProvider.close();
    }

    /** A service factory, and whether it was loaded from a narrowed lib/. */
    @VisibleForTesting
    static final class LoadedServiceFactory
    {
        final IPrestoSparkServiceFactory factory;
        final boolean narrowed;

        private LoadedServiceFactory(IPrestoSparkServiceFactory factory, boolean narrowed)
        {
            this.factory = requireNonNull(factory, "factory is null");
            this.narrowed = narrowed;
        }
    }

    /** The service factory, from {@code narrowed} (the copy of lib/) if possible, else from {@code directory}. */
    @VisibleForTesting
    static LoadedServiceFactory createServiceFactory(File directory, Optional<File> narrowed)
    {
        if (narrowed.isPresent()) {
            // Fall back if the copy lacks the provider or a class it needs (a bare LinkageError);
            // later failures are retried by startService.
            PrestoSparkLoader narrowedLoader = null;
            try {
                narrowedLoader = createLoader(linkTargets(narrowed.get()));
                Iterator<IPrestoSparkServiceFactory> providers = ServiceLoader.load(IPrestoSparkServiceFactory.class, narrowedLoader).iterator();
                if (providers.hasNext()) {
                    return new LoadedServiceFactory(providers.next(), true);
                }
                System.err.println("[presto-spark] " + narrowed.get() + " lacks the " + IPrestoSparkServiceFactory.class.getSimpleName()
                        + " provider; using " + directory);
            }
            catch (ServiceConfigurationError | LinkageError | UncheckedIOException e) {
                System.err.println("[presto-spark] could not load the " + IPrestoSparkServiceFactory.class.getSimpleName() + " provider from "
                        + narrowed.get() + "; using " + directory + ". Cause: " + e);
            }
            if (narrowedLoader != null) {
                try {
                    narrowedLoader.close();
                }
                catch (IOException e) {
                    // nothing to do: the loader is discarded either way
                }
            }
        }
        checkDirectory(directory);
        File[] files = listFiles(directory);
        sort(files);
        List<URL> urls = new ArrayList<>();
        for (File file : files) {
            urls.add(toUrl(file.toPath()));
        }
        return new LoadedServiceFactory(ServiceLoader.load(IPrestoSparkServiceFactory.class, createLoader(urls)).iterator().next(), false);
    }

    /** The link targets, resolved lexically, so the jars open by their lib/ paths. */
    private static List<URL> linkTargets(File narrowed)
    {
        File[] links = listFiles(narrowed);
        sort(links);
        List<URL> urls = new ArrayList<>();
        for (File link : links) {
            try {
                urls.add(toUrl(link.toPath().toAbsolutePath().resolveSibling(Files.readSymbolicLink(link.toPath())).normalize()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return urls;
    }

    private static URL toUrl(Path file)
    {
        try {
            return file.toUri().toURL();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static PrestoSparkLoader createLoader(List<URL> urls)
    {
        return new PrestoSparkLoader(
                urls,
                PrestoSparkLauncher.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.classloader_interface.", "scala.", "com.facebook.di.security.token_service."));
    }

    /** A service, and whether it runs on a narrowed classpath. */
    @VisibleForTesting
    static final class CreatedService
    {
        final IPrestoSparkService service;
        final boolean classpathNarrowed;

        private CreatedService(IPrestoSparkService service, boolean classpathNarrowed)
        {
            this.service = requireNonNull(service, "service is null");
            this.classpathNarrowed = classpathNarrowed;
        }
    }

    private static CreatedService createService(
            SparkProcessType sparkProcessType,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Map<String, String> prestoSparkProperties,
            Optional<Map<String, String>> nativeWorkerConfigProperties,
            Optional<Map<String, Map<String, String>>> nativeWorkerCatalogConfigProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            Optional<Map<String, Map<String, String>>> expressionManagerProperties,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector,
            boolean narrowClasspath,
            Runnable onRetryFailure)
    {
        PrestoSparkBootstrapTimer bootstrapTimer = new PrestoSparkBootstrapTimer(systemTicker(), !sparkProcessType.equals(SparkProcessType.DRIVER));
        bootstrapTimer.beginRunnerServiceCreation();

        String packagePath = getPackagePath(packageSupplier);
        File pluginsRoot = checkDirectory(new File(packagePath, "plugin"));
        File libDirectory = checkDirectory(new File(packagePath, "lib"));
        Map<String, String> serviceConfig = NarrowedExecutorClasspath.serviceConfig(configProperties);
        Function<File, PrestoSparkConfiguration> configuration = pluginsDirectory -> new PrestoSparkConfiguration(
                serviceConfig,
                pluginsDirectory.getAbsolutePath(),
                catalogProperties,
                prestoSparkProperties,
                nativeWorkerConfigProperties,
                nativeWorkerCatalogConfigProperties,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties,
                expressionManagerProperties);

        Optional<File> narrowedLib = Optional.empty();
        Optional<File> narrowedPlugins = Optional.empty();
        if (narrowClasspath) {
            narrowedLib = NarrowedExecutorClasspath.narrowedLib(libDirectory, System.err);
            narrowedPlugins = NarrowedExecutorClasspath.narrowedPlugins(pluginsRoot, System.err);
            if (!narrowedLib.isPresent() && !narrowedPlugins.isPresent()) {
                System.err.println(format("[presto-spark] %s is set, but the package has no usable %s or %s; using the full classpath",
                        NarrowedExecutorClasspath.CONFIG_PROPERTY, NarrowedExecutorClasspath.LIB_DIRECTORY, NarrowedExecutorClasspath.PLUGIN_DIRECTORY));
            }
        }
        LoadedServiceFactory serviceFactory = createServiceFactory(libDirectory, narrowedLib);
        File pluginsDirectory = narrowedPlugins.orElse(pluginsRoot);
        CreatedService created = startService(
                serviceFactory.narrowed || narrowedPlugins.isPresent(),
                () -> serviceFactory.factory.createService(sparkProcessType, configuration.apply(pluginsDirectory), bootstrapTimer),
                () -> createServiceFactory(libDirectory, Optional.empty()).factory
                        .createService(sparkProcessType, configuration.apply(pluginsRoot), bootstrapTimer),
                PrestoSparkRunner::isTaskInterrupted,
                onRetryFailure,
                System.err);
        bootstrapTimer.endRunnerServiceCreation();
        if (bootstrapMetricsCollector.isPresent() && bootstrapTimer.isExecutorBootstrap()) {
            bootstrapMetricsCollector.get().add(bootstrapTimer.exportBootstrapDurations());
        }
        return created;
    }

    /**
     * Starts the service, and if that fails on a narrowed classpath, once more on the full one: a
     * left-out jar may be needed only once the service starts. A failure the narrowing did not
     * cause fails the retry too, and is thrown with the first one suppressed. The failed attempt is
     * not stopped and stays until the executor exits.
     *
     * <p>Not retried if the task is being killed (the interrupt, not the classpath, failed it). If
     * the retry fails too, {@code onRetryFailure} runs, unless the task is being killed, so later
     * tasks use the full classpath only.
     */
    @VisibleForTesting
    static CreatedService startService(
            boolean narrowed,
            Supplier<IPrestoSparkService> first,
            Supplier<IPrestoSparkService> onFullClasspath,
            BooleanSupplier interrupted,
            Runnable onRetryFailure,
            PrintStream err)
    {
        try {
            return new CreatedService(first.get(), narrowed);
        }
        catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
            if (!narrowed || interrupted.getAsBoolean()) {
                throw e;
            }
            err.println("[presto-spark] the Presto on Spark service failed to start on the classpath narrowed by "
                    + NarrowedExecutorClasspath.CONFIG_PROPERTY + "; retrying once with the full classpath. Cause:");
            e.printStackTrace(err);
            try {
                return new CreatedService(onFullClasspath.get(), false);
            }
            catch (RuntimeException | Error retryFailure) {
                retryFailure.addSuppressed(e);
                if (!interrupted.getAsBoolean()) {
                    onRetryFailure.run();
                }
                throw retryFailure;
            }
        }
    }

    private static boolean isTaskInterrupted()
    {
        // Spark marks the task killed before interrupting its thread
        TaskContext task = TaskContext.get();
        return Thread.currentThread().isInterrupted() || (task != null && task.isInterrupted());
    }

    private static String getPackagePath(PackageSupplier packageSupplier)
    {
        return checkDirectory(packageSupplier.getPrestoSparkPackageDirectory()).getAbsolutePath();
    }

    private static class DistributionBasedPrestoSparkTaskExecutorFactoryProvider
            implements PrestoSparkTaskExecutorFactoryProvider
    {
        private final PackageSupplier packageSupplier;
        private final Map<String, String> configProperties;
        private final Map<String, String> nativeWorkerConfigProperties;
        private final Map<String, Map<String, String>> catalogProperties;
        private final Map<String, Map<String, String>> nativeWorkerCatalogConfigProperties;
        private final Map<String, String> prestoSparkProperties;
        private final Map<String, String> eventListenerProperties;
        private final Map<String, String> accessControlProperties;
        private final Map<String, String> sessionPropertyConfigurationProperties;
        private final Map<String, Map<String, String>> functionNamespaceProperties;
        private final Map<String, Map<String, String>> tempStorageProperties;
        private final Map<String, Map<String, String>> expressionManagerProperties;
        private final CollectionAccumulator<Map<String, Long>> bootstrapMetricsCollector;
        private final boolean isLocal;

        public DistributionBasedPrestoSparkTaskExecutorFactoryProvider(
                PrestoSparkDistribution distribution,
                CollectionAccumulator<Map<String, Long>> bootstrapMetricsCollector)
        {
            requireNonNull(distribution, "distribution is null");
            this.packageSupplier = distribution.getPackageSupplier();
            this.configProperties = distribution.getConfigProperties();
            this.nativeWorkerConfigProperties = distribution.getNativeWorkerConfigProperties().orElse(null);
            this.catalogProperties = distribution.getCatalogProperties();
            this.nativeWorkerCatalogConfigProperties = distribution.getNativeWorkerCatalogConfigProperties().orElse(null);
            this.prestoSparkProperties = distribution.getPrestoSparkProperties();
            this.bootstrapMetricsCollector = requireNonNull(bootstrapMetricsCollector);
            // Optional is not Serializable
            this.eventListenerProperties = distribution.getEventListenerProperties().orElse(null);
            this.accessControlProperties = distribution.getAccessControlProperties().orElse(null);
            this.sessionPropertyConfigurationProperties = distribution.getSessionPropertyConfigurationProperties().orElse(null);
            this.functionNamespaceProperties = distribution.getFunctionNamespaceProperties().orElse(null);
            this.tempStorageProperties = distribution.getTempStorageProperties().orElse(null);
            this.expressionManagerProperties = distribution.getExpressionManagerProperties().orElse(null);
            this.isLocal = distribution.getSparkContext().isLocal();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            CreatedService created = getOrCreatePrestoSparkService();
            NarrowedExecutorClasspath.checkJavaEngineAllowed(created.classpathNarrowed);
            return created.service.getTaskExecutorFactory();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory getNative()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            return getOrCreatePrestoSparkService().service.getNativeTaskExecutorFactory();
        }

        private static CreatedService service;
        // set once both classpaths failed: later tasks use the full classpath only
        private static boolean fullClasspathOnly;
        private static String currentPackagePath;
        private static Map<String, String> currentConfigProperties;
        private static Map<String, String> currentNativeWorkerConfigProperties;
        private static Map<String, Map<String, String>> currentCatalogProperties;
        private static Map<String, Map<String, String>> currentNativeWorkerCatalogConfigProperties;
        private static Map<String, String> currentPrestoSparkProperties;
        private static Map<String, String> currentEventListenerProperties;
        private static Map<String, String> currentAccessControlProperties;
        private static Map<String, String> currentSessionPropertyConfigurationProperties;
        private static Map<String, Map<String, String>> currentFunctionNamespaceProperties;
        private static Map<String, Map<String, String>> currentTempStorageProperties;
        private static Map<String, Map<String, String>> currentExpressionManagerProperties;

        private CreatedService getOrCreatePrestoSparkService()
        {
            synchronized (DistributionBasedPrestoSparkTaskExecutorFactoryProvider.class) {
                if (service == null) {
                    SparkProcessType sparkProcessType = isLocal ? SparkProcessType.LOCAL_EXECUTOR : SparkProcessType.EXECUTOR;
                    String packagePath = getPackagePath(packageSupplier);
                    boolean narrowClasspath = NarrowedExecutorClasspath.isEnabled(sparkProcessType, configProperties, nativeWorkerConfigProperties != null);
                    if (narrowClasspath && fullClasspathOnly) {
                        System.err.println("[presto-spark] the Presto on Spark service failed to start on both the narrowed and the full classpath "
                                + "earlier on this executor; starting it on the full classpath only");
                        narrowClasspath = false;
                    }
                    service = createService(
                            sparkProcessType,
                            packageSupplier,
                            configProperties,
                            catalogProperties,
                            prestoSparkProperties,
                            Optional.ofNullable(nativeWorkerConfigProperties),
                            Optional.ofNullable(nativeWorkerCatalogConfigProperties),
                            Optional.ofNullable(eventListenerProperties),
                            Optional.ofNullable(accessControlProperties),
                            Optional.ofNullable(sessionPropertyConfigurationProperties),
                            Optional.ofNullable(functionNamespaceProperties),
                            Optional.ofNullable(tempStorageProperties),
                            Optional.ofNullable(expressionManagerProperties),
                            Optional.of(bootstrapMetricsCollector),
                            narrowClasspath,
                            () -> fullClasspathOnly = true);

                    currentPackagePath = packagePath;
                    currentConfigProperties = configProperties;
                    currentNativeWorkerConfigProperties = nativeWorkerConfigProperties;
                    currentCatalogProperties = catalogProperties;
                    currentNativeWorkerCatalogConfigProperties = nativeWorkerCatalogConfigProperties;
                    currentPrestoSparkProperties = prestoSparkProperties;
                    currentEventListenerProperties = eventListenerProperties;
                    currentAccessControlProperties = accessControlProperties;
                    currentSessionPropertyConfigurationProperties = sessionPropertyConfigurationProperties;
                    currentFunctionNamespaceProperties = functionNamespaceProperties;
                    currentTempStorageProperties = tempStorageProperties;
                    currentExpressionManagerProperties = expressionManagerProperties;
                }
                else {
                    checkEquals("packagePath", currentPackagePath, getPackagePath(packageSupplier));
                    checkEquals("configProperties", currentConfigProperties, configProperties);
                    checkEquals("nativeWorkerConfigProperties", currentNativeWorkerConfigProperties, nativeWorkerConfigProperties);
                    checkEquals("catalogProperties", currentCatalogProperties, catalogProperties);
                    checkEquals("nativeWorkerCatalogConfigProperties", currentNativeWorkerCatalogConfigProperties, nativeWorkerCatalogConfigProperties);
                    checkEquals("prestoSparkProperties", currentPrestoSparkProperties, prestoSparkProperties);
                    checkEquals("eventListenerProperties", currentEventListenerProperties, eventListenerProperties);
                    checkEquals("accessControlProperties", currentAccessControlProperties, accessControlProperties);
                    checkEquals("sessionPropertyConfigurationProperties",
                            currentSessionPropertyConfigurationProperties,
                            sessionPropertyConfigurationProperties);
                    checkEquals("functionNamespaceProperties", currentFunctionNamespaceProperties, functionNamespaceProperties);
                    checkEquals("tempStorageProperties", currentTempStorageProperties, tempStorageProperties);
                    checkEquals("expressionManagerProperties", currentExpressionManagerProperties, expressionManagerProperties);
                }
                return service;
            }
        }

        public static void checkEquals(String name, Object first, Object second)
        {
            if (!Objects.equals(first, second)) {
                throw new IllegalStateException(format("%s is different: %s != %s", name, first, second));
            }
        }

        public static synchronized void close()
        {
            if (service != null) {
                service.service.close();
            }
        }
    }
}
