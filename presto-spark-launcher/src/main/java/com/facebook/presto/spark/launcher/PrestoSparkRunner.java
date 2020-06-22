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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.TaskContext;

import java.io.File;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

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
                distribution.getCatalogProperties());
    }

    public void run(
            String catalog,
            String schema,
            String query,
            Map<String, String> sessionProperties,
            Map<String, Map<String, String>> catalogSessionProperties)
    {
        IPrestoSparkQueryExecutionFactory queryExecutionFactory = driverPrestoSparkService.getQueryExecutionFactory();
        PrestoSparkSession session = createSessionInfo(catalog, schema, sessionProperties, catalogSessionProperties);
        IPrestoSparkQueryExecution queryExecution = queryExecutionFactory.create(
                distribution.getSparkContext(),
                session,
                query,
                new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(distribution));

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
    }

    @Override
    public void close()
    {
        driverPrestoSparkService.close();
    }

    private static PrestoSparkSession createSessionInfo(
            String catalog,
            String schema,
            Map<String, String> sessionProperties,
            Map<String, Map<String, String>> catalogSessionProperties)
    {
        // TODO: add all important session parameters to client options
        return new PrestoSparkSession(
                "test",
                Optional.empty(),
                ImmutableMap.of(),
                Optional.ofNullable(catalog),
                Optional.ofNullable(schema),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                sessionProperties,
                catalogSessionProperties,
                Optional.empty());
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
                asList("org.apache.spark.", "com.facebook.presto.spark.classloader_interface.", "scala."));
        ServiceLoader<IPrestoSparkServiceFactory> serviceLoader = ServiceLoader.load(IPrestoSparkServiceFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    private static IPrestoSparkService createService(
            SparkProcessType sparkProcessType,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties)
    {
        String packagePath = getPackagePath(packageSupplier);
        File pluginsDirectory = checkDirectory(new File(packagePath, "plugin"));
        PrestoSparkConfiguration configuration = new PrestoSparkConfiguration(configProperties, pluginsDirectory.getAbsolutePath(), catalogProperties);
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
        private final Map<String, String> configProperties;
        private final Map<String, Map<String, String>> catalogProperties;

        public DistributionBasedPrestoSparkTaskExecutorFactoryProvider(PrestoSparkDistribution distribution)
        {
            requireNonNull(distribution, "distribution is null");
            this.packageSupplier = distribution.getPackageSupplier();
            this.configProperties = distribution.getConfigProperties();
            this.catalogProperties = distribution.getCatalogProperties();
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
        private static Map<String, String> currentConfigProperties;
        private static Map<String, Map<String, String>> currentCatalogProperties;

        private IPrestoSparkService getOrCreatePrestoSparkService()
        {
            synchronized (DistributionBasedPrestoSparkTaskExecutorFactoryProvider.class) {
                if (service == null) {
                    service = createService(SparkProcessType.EXECUTOR, packageSupplier, configProperties, catalogProperties);
                    currentPackagePath = getPackagePath(packageSupplier);
                    currentConfigProperties = configProperties;
                    currentCatalogProperties = catalogProperties;
                }
                checkEquals("packagePath", currentPackagePath, getPackagePath(packageSupplier));
                checkEquals("configProperties", currentConfigProperties, configProperties);
                checkEquals("catalogProperties", currentCatalogProperties, catalogProperties);
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
