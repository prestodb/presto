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
import com.google.common.base.Splitter;
import org.apache.spark.TaskContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Option;

import java.io.File;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.launcher.LauncherUtils.checkDirectory;
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
                distribution.getEventListenerProperties(),
                distribution.getAccessControlProperties(),
                distribution.getSessionPropertyConfigurationProperties(),
                distribution.getFunctionNamespaceProperties(),
                distribution.getTempStorageProperties(),
                Optional.empty());
    }

    public void run(
            String user,
            Optional<Principal> principal,
            Map<String, String> extraCredentials,
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

    private static IPrestoSparkServiceFactory createServiceFactory(File directory)
    {
        checkDirectory(directory);
        List<URL> urls = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files != null) {
            sort(files);
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new UncheckedIOException(e);
            }
        }
        PrestoSparkLoader prestoSparkLoader = new PrestoSparkLoader(
                urls,
                PrestoSparkLauncher.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.classloader_interface.", "scala.", "com.facebook.di.security.token_service."));
        ServiceLoader<IPrestoSparkServiceFactory> serviceLoader = ServiceLoader.load(IPrestoSparkServiceFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    private static IPrestoSparkService createService(
            SparkProcessType sparkProcessType,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Map<String, String> prestoSparkProperties,
            Optional<Map<String, String>> nativeWorkerConfigProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector)
    {
        PrestoSparkBootstrapTimer bootstrapTimer = new PrestoSparkBootstrapTimer(systemTicker(), !sparkProcessType.equals(SparkProcessType.DRIVER));
        bootstrapTimer.beginRunnerServiceCreation();

        String packagePath = getPackagePath(packageSupplier);
        File pluginsDirectory = checkDirectory(new File(packagePath, "plugin"));
        PrestoSparkConfiguration configuration = new PrestoSparkConfiguration(
                configProperties,
                pluginsDirectory.getAbsolutePath(),
                catalogProperties,
                prestoSparkProperties,
                nativeWorkerConfigProperties,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties);
        IPrestoSparkServiceFactory serviceFactory = createServiceFactory(checkDirectory(new File(packagePath, "lib")));
        IPrestoSparkService service = serviceFactory.createService(sparkProcessType, configuration, bootstrapTimer);
        bootstrapTimer.endRunnerServiceCreation();
        if (bootstrapMetricsCollector.isPresent() && bootstrapTimer.isExecutorBootstrap()) {
            bootstrapMetricsCollector.get().add(bootstrapTimer.exportBootstrapDurations());
        }
        return service;
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
        private final Map<String, String> prestoSparkProperties;
        private final Map<String, String> eventListenerProperties;
        private final Map<String, String> accessControlProperties;
        private final Map<String, String> sessionPropertyConfigurationProperties;
        private final Map<String, Map<String, String>> functionNamespaceProperties;
        private final Map<String, Map<String, String>> tempStorageProperties;
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
            this.prestoSparkProperties = distribution.getPrestoSparkProperties();
            this.bootstrapMetricsCollector = requireNonNull(bootstrapMetricsCollector);
            // Optional is not Serializable
            this.eventListenerProperties = distribution.getEventListenerProperties().orElse(null);
            this.accessControlProperties = distribution.getAccessControlProperties().orElse(null);
            this.sessionPropertyConfigurationProperties = distribution.getSessionPropertyConfigurationProperties().orElse(null);
            this.functionNamespaceProperties = distribution.getFunctionNamespaceProperties().orElse(null);
            this.tempStorageProperties = distribution.getTempStorageProperties().orElse(null);
            this.isLocal = distribution.getSparkContext().isLocal();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            IPrestoSparkService prestoSparkService = getOrCreatePrestoSparkService();
            return prestoSparkService.getTaskExecutorFactory();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory getNative()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            IPrestoSparkService prestoSparkService = getOrCreatePrestoSparkService();
            return prestoSparkService.getNativeTaskExecutorFactory();
        }

        private static IPrestoSparkService service;
        private static String currentPackagePath;
        private static Map<String, String> currentConfigProperties;
        private static Map<String, String> currentNativeWorkerConfigProperties;
        private static Map<String, Map<String, String>> currentCatalogProperties;
        private static Map<String, String> currentPrestoSparkProperties;
        private static Map<String, String> currentEventListenerProperties;
        private static Map<String, String> currentAccessControlProperties;
        private static Map<String, String> currentSessionPropertyConfigurationProperties;
        private static Map<String, Map<String, String>> currentFunctionNamespaceProperties;
        private static Map<String, Map<String, String>> currentTempStorageProperties;

        private IPrestoSparkService getOrCreatePrestoSparkService()
        {
            synchronized (DistributionBasedPrestoSparkTaskExecutorFactoryProvider.class) {
                if (service == null) {
                    service = createService(
                            isLocal ? SparkProcessType.LOCAL_EXECUTOR : SparkProcessType.EXECUTOR,
                            packageSupplier,
                            configProperties,
                            catalogProperties,
                            prestoSparkProperties,
                            Optional.ofNullable(nativeWorkerConfigProperties),
                            Optional.ofNullable(eventListenerProperties),
                            Optional.ofNullable(accessControlProperties),
                            Optional.ofNullable(sessionPropertyConfigurationProperties),
                            Optional.ofNullable(functionNamespaceProperties),
                            Optional.ofNullable(tempStorageProperties),
                            Optional.of(bootstrapMetricsCollector));

                    currentPackagePath = getPackagePath(packageSupplier);
                    currentConfigProperties = configProperties;
                    currentNativeWorkerConfigProperties = nativeWorkerConfigProperties;
                    currentCatalogProperties = catalogProperties;
                    currentPrestoSparkProperties = prestoSparkProperties;
                    currentEventListenerProperties = eventListenerProperties;
                    currentAccessControlProperties = accessControlProperties;
                    currentSessionPropertyConfigurationProperties = sessionPropertyConfigurationProperties;
                    currentFunctionNamespaceProperties = functionNamespaceProperties;
                    currentTempStorageProperties = tempStorageProperties;
                }
                else {
                    checkEquals("packagePath", currentPackagePath, getPackagePath(packageSupplier));
                    checkEquals("configProperties", currentConfigProperties, configProperties);
                    checkEquals("nativeWorkerConfigProperties", currentNativeWorkerConfigProperties, nativeWorkerConfigProperties);
                    checkEquals("catalogProperties", currentCatalogProperties, catalogProperties);
                    checkEquals("prestoSparkProperties", currentPrestoSparkProperties, prestoSparkProperties);
                    checkEquals("eventListenerProperties", currentEventListenerProperties, eventListenerProperties);
                    checkEquals("accessControlProperties", currentAccessControlProperties, accessControlProperties);
                    checkEquals("sessionPropertyConfigurationProperties",
                            currentSessionPropertyConfigurationProperties,
                            sessionPropertyConfigurationProperties);
                    checkEquals("functionNamespaceProperties", currentFunctionNamespaceProperties, functionNamespaceProperties);
                    checkEquals("tempStorageProperties", currentTempStorageProperties, tempStorageProperties);
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
                service.close();
            }
        }
    }
}
