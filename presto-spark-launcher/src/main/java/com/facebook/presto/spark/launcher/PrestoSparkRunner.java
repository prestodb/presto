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

import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import org.apache.spark.TaskContext;

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

import static com.facebook.presto.spark.launcher.LauncherUtils.checkDirectory;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRunner
        implements AutoCloseable
{
    private final PrestoSparkDistribution distribution;
    private final IPrestoSparkService driverPrestoSparkService;

    public PrestoSparkRunner(PrestoSparkDistribution distribution)
    {
        this.distribution = requireNonNull(distribution, "distribution is null");
        driverPrestoSparkService = createService(
                SparkProcessType.DRIVER,
                distribution.getPackageSupplier(),
                distribution.getConfigProperties(),
                distribution.getCatalogProperties(),
                distribution.getMetadataStorageType(),
                distribution.getEventListenerProperties(),
                distribution.getAccessControlProperties(),
                distribution.getSessionPropertyConfigurationProperties(),
                distribution.getFunctionNamespaceProperties(),
                distribution.getTempStorageProperties());
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

        PrestoSparkSession session = new PrestoSparkSession(
                user,
                principal,
                extraCredentials,
                Optional.ofNullable(catalog),
                Optional.ofNullable(schema),
                source,
                userAgent,
                clientInfo,
                clientTags,
                Optional.empty(),
                Optional.empty(),
                sessionProperties,
                catalogSessionProperties,
                traceToken);

        IPrestoSparkQueryExecution queryExecution = queryExecutionFactory.create(
                distribution.getSparkContext(),
                session,
                sqlText,
                sqlLocation,
                sqlFileHexHash,
                sqlFileSizeInBytes,
                sparkQueueName,
                new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(distribution),
                queryStatusInfoOutputLocation,
                queryDataOutputLocation);

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
    }

    @Override
    public void close()
    {
        driverPrestoSparkService.close();
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
            String metadataStorageType,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties)
    {
        String packagePath = getPackagePath(packageSupplier);
        File pluginsDirectory = checkDirectory(new File(packagePath, "plugin"));
        PrestoSparkConfiguration configuration = new PrestoSparkConfiguration(
                configProperties,
                pluginsDirectory.getAbsolutePath(),
                catalogProperties,
                metadataStorageType,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties);
        IPrestoSparkServiceFactory serviceFactory = createServiceFactory(checkDirectory(new File(packagePath, "lib")));
        return serviceFactory.createService(sparkProcessType, configuration);
    }

    private static String getPackagePath(PackageSupplier packageSupplier)
    {
        return checkDirectory(packageSupplier.getPrestoSparkPackageDirectory()).getAbsolutePath();
    }

    private static class DistributionBasedPrestoSparkTaskExecutorFactoryProvider
            implements PrestoSparkTaskExecutorFactoryProvider
    {
        private final PackageSupplier packageSupplier;
        private final String metadataStorageType;
        private final Map<String, String> configProperties;
        private final Map<String, Map<String, String>> catalogProperties;
        private final Map<String, String> eventListenerProperties;
        private final Map<String, String> accessControlProperties;
        private final Map<String, String> sessionPropertyConfigurationProperties;
        private final Map<String, Map<String, String>> functionNamespaceProperties;
        private final Map<String, Map<String, String>> tempStorageProperties;

        public DistributionBasedPrestoSparkTaskExecutorFactoryProvider(PrestoSparkDistribution distribution)
        {
            requireNonNull(distribution, "distribution is null");
            this.packageSupplier = distribution.getPackageSupplier();
            this.metadataStorageType = distribution.getMetadataStorageType();
            this.configProperties = distribution.getConfigProperties();
            this.catalogProperties = distribution.getCatalogProperties();
            // Optional is not Serializable
            this.eventListenerProperties = distribution.getEventListenerProperties().orElse(null);
            this.accessControlProperties = distribution.getAccessControlProperties().orElse(null);
            this.sessionPropertyConfigurationProperties = distribution.getSessionPropertyConfigurationProperties().orElse(null);
            this.functionNamespaceProperties = distribution.getFunctionNamespaceProperties().orElse(null);
            this.tempStorageProperties = distribution.getTempStorageProperties().orElse(null);
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            IPrestoSparkService prestoSparkService = getOrCreatePrestoSparkService();
            return prestoSparkService.getTaskExecutorFactory();
        }

        private static IPrestoSparkService service;
        private static String currentPackagePath;
        private static String currentMetadataStorageType;
        private static Map<String, String> currentConfigProperties;
        private static Map<String, Map<String, String>> currentCatalogProperties;
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
                            SparkProcessType.EXECUTOR,
                            packageSupplier,
                            configProperties,
                            catalogProperties,
                            metadataStorageType,
                            Optional.ofNullable(eventListenerProperties),
                            Optional.ofNullable(accessControlProperties),
                            Optional.ofNullable(sessionPropertyConfigurationProperties),
                            Optional.ofNullable(functionNamespaceProperties),
                            Optional.ofNullable(tempStorageProperties));

                    currentMetadataStorageType = metadataStorageType;
                    currentPackagePath = getPackagePath(packageSupplier);
                    currentConfigProperties = configProperties;
                    currentCatalogProperties = catalogProperties;
                    currentEventListenerProperties = eventListenerProperties;
                    currentAccessControlProperties = accessControlProperties;
                    currentSessionPropertyConfigurationProperties = sessionPropertyConfigurationProperties;
                    currentFunctionNamespaceProperties = functionNamespaceProperties;
                    currentTempStorageProperties = tempStorageProperties;
                }
                else {
                    checkEquals("packagePath", currentPackagePath, getPackagePath(packageSupplier));
                    checkEquals("metadataStorageType", currentMetadataStorageType, metadataStorageType);
                    checkEquals("configProperties", currentConfigProperties, configProperties);
                    checkEquals("catalogProperties", currentCatalogProperties, catalogProperties);
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
    }
}
