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
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.fromProperties;
import static com.google.common.hash.Hashing.combineOrdered;
import static com.google.common.io.Files.asByteSource;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Command(name = "presto-spark-launcher", description = "Presto on Spark launcher")
public class PrestoSparkLauncherCommand
{
    private static final Logger log = Logger.getLogger(PrestoSparkLauncherCommand.class.getName());

    @Inject
    public HelpOption helpOption;

    @Inject
    public PrestoSparkVersionOption versionOption = new PrestoSparkVersionOption();

    @Inject
    public PrestoSparkClientOptions clientOptions = new PrestoSparkClientOptions();

    public void run(SparkContextFactory sparkContextFactory)
    {
        String query = readFileUtf8(checkFile(new File(clientOptions.file)));

        PrestoSparkDistribution distribution = createDistribution(clientOptions);

        SparkContext sparkContext = sparkContextFactory.create(clientOptions);

        distribution.deploy(sparkContext);

        CachingServiceFactory serviceFactory = new CachingServiceFactory(distribution);
        IPrestoSparkService service = serviceFactory.createService();
        IPrestoSparkQueryExecutionFactory queryExecutionFactory = service.getQueryExecutionFactory();
        PrestoSparkSession session = createSessionInfo(clientOptions);
        IPrestoSparkQueryExecution queryExecution = queryExecutionFactory.create(sparkContext, session, query, new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(serviceFactory));

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
    }

    private static PrestoSparkDistribution createDistribution(PrestoSparkClientOptions clientOptions)
    {
        return new PrestoSparkDistribution(
                new File(clientOptions.packagePath),
                loadProperties(checkFile(new File(clientOptions.config))),
                loadCatalogProperties(checkDirectory(new File(clientOptions.catalogs))));
    }

    private static PrestoSparkSession createSessionInfo(PrestoSparkClientOptions clientOptions)
    {
        // TODO: add all important session parameters to client options
        return new PrestoSparkSession(
                "test",
                Optional.empty(),
                ImmutableMap.of(),
                Optional.ofNullable(clientOptions.catalog),
                Optional.ofNullable(clientOptions.schema),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableMap.of(),
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

    private static PrestoSparkConfiguration createConfiguration(PrestoSparkDistribution distribution)
    {
        return new PrestoSparkConfiguration(
                distribution.getConfigProperties(),
                new File(distribution.getLocalPackageDirectory(), "plugin").getAbsolutePath(),
                distribution.getCatalogProperties());
    }

    private static File checkFile(File file)
    {
        checkArgument(file.exists() && file.isFile(), "file does not exist: %s", file);
        checkArgument(file.canRead(), "file is not readable: %s", file);
        return file;
    }

    private static File checkDirectory(File directory)
    {
        checkArgument(directory.exists() && directory.isDirectory(), "directory does not exist: %s", directory);
        checkArgument(directory.canRead() && directory.canExecute(), "directory is not readable: %s", directory);
        return directory;
    }

    private static String readFileUtf8(File file)
    {
        try {
            return asCharSource(file, UTF_8).read();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Map<String, Map<String, String>> loadCatalogProperties(File catalogsDirectory)
    {
        ImmutableMap.Builder<String, Map<String, String>> result = ImmutableMap.builder();
        for (File file : catalogsDirectory.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                result.put(getNameWithoutExtension(file.getName()), loadProperties(file));
            }
        }
        return result.build();
    }

    public static Map<String, String> loadProperties(File file)
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fromProperties(properties);
    }

    public static class PrestoSparkDistribution
            implements Serializable
    {
        private final File packageFile;
        private final Map<String, String> configProperties;
        private final Map<String, Map<String, String>> catalogProperties;
        private final String fingerprint;

        public PrestoSparkDistribution(
                File packageFile,
                Map<String, String> configProperties,
                Map<String, Map<String, String>> catalogProperties)
        {
            this.packageFile = checkFile(requireNonNull(packageFile, "packageFile is null"));
            this.configProperties = ImmutableMap.copyOf(requireNonNull(configProperties, "configProperties is null"));
            this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
            this.fingerprint = computeFingerprint(packageFile, configProperties, catalogProperties);
        }

        private static String computeFingerprint(
                File packageFile,
                Map<String, String> configProperties,
                Map<String, Map<String, String>> catalogProperties)
        {
            try {
                ImmutableList.Builder<HashCode> hashes = ImmutableList.builder();
                hashes.add(asByteSource(packageFile).hash(Hashing.sha256()));
                ByteBuffer byteBuffer = ByteBuffer.allocate(32);
                byteBuffer.putInt(configProperties.hashCode());
                byteBuffer.putInt(catalogProperties.hashCode());
                hashes.add(HashCode.fromBytes(byteBuffer.array()));
                return combineOrdered(hashes.build()).toString();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void deploy(SparkContext context)
        {
            context.addFile(packageFile.getAbsolutePath());
        }

        public File getLocalPackageDirectory()
        {
            File localPackageFile = packageFile.exists() ? packageFile : getLocalFile(packageFile.getName());
            return ensureDecompressed(localPackageFile, new File(SparkFiles.getRootDirectory()));
        }

        public Map<String, String> getConfigProperties()
        {
            return configProperties;
        }

        public Map<String, Map<String, String>> getCatalogProperties()
        {
            return catalogProperties;
        }

        public String getFingerprint()
        {
            return fingerprint;
        }

        private static File ensureDecompressed(File archive, File outputDirectory)
        {
            String packageDirectoryName = TarGz.getRootDirectoryName(archive);
            File packageDirectory = new File(outputDirectory, packageDirectoryName);
            log.info(format("Package directory: %s", packageDirectory));
            if (packageDirectory.exists()) {
                verify(packageDirectory.isDirectory(), "package directory is not a directory: %s", packageDirectory);
                log.info(format("Skipping decompression step as package is already decompressed: %s", packageDirectory));
                return packageDirectory;
            }
            Stopwatch stopwatch = Stopwatch.createStarted();
            log.info(format("Decompressing: %s", packageDirectory));
            TarGz.extract(archive, outputDirectory);
            log.info(format("Decompression took: %sms", stopwatch.elapsed(MILLISECONDS)));
            return packageDirectory;
        }

        private static File getLocalFile(String name)
        {
            String path = requireNonNull(SparkFiles.get(name), "path is null");
            return checkFile(new File(path));
        }
    }

    public static class CachingServiceFactory
            implements Serializable
    {
        private static final Cache<String, IPrestoSparkService> services = CacheBuilder.newBuilder().build();

        private final PrestoSparkDistribution distribution;

        public CachingServiceFactory(PrestoSparkDistribution distribution)
        {
            this.distribution = requireNonNull(distribution, "distribution is null");
        }

        public IPrestoSparkService createService()
        {
            try {
                return services.get(distribution.getFingerprint(), () -> {
                    IPrestoSparkServiceFactory serviceFactory = createServiceFactory(new File(distribution.getLocalPackageDirectory(), "lib"));
                    return serviceFactory.createService(createConfiguration(distribution));
                });
            }
            catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            }
        }
    }

    public static class DistributionBasedPrestoSparkTaskExecutorFactoryProvider
            implements PrestoSparkTaskExecutorFactoryProvider
    {
        private final CachingServiceFactory serviceFactory;

        public DistributionBasedPrestoSparkTaskExecutorFactoryProvider(CachingServiceFactory serviceFactory)
        {
            this.serviceFactory = requireNonNull(serviceFactory, "serviceFactory is null");
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            return serviceFactory.createService().getTaskExecutorFactory();
        }
    }
}
