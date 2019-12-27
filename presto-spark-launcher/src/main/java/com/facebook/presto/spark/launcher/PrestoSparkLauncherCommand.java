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

import com.facebook.presto.spark.classloader_interface.IPrestoSparkExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskCompiler;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskCompilerFactory;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.combineOrdered;
import static com.google.common.io.Files.asByteSource;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
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

    public void run(SparkContextFactory sparkContextFactory, PrestoSparkSessionFactory prestoSparkSessionFactory)
    {
        String query = readFileUtf8(checkFile(new File(clientOptions.file)));

        PrestoSparkDistribution distribution = createDistribution(clientOptions);

        SparkContext sparkContext = sparkContextFactory.createSparkContext(clientOptions);

        File tempDir = createTempDir();
        distribution.deploy(sparkContext, tempDir);

        CachingServiceFactory serviceFactory = new CachingServiceFactory(distribution);
        IPrestoSparkService service = serviceFactory.createService();
        IPrestoSparkExecutionFactory executionFactory = service.createExecutionFactory();
        PrestoSparkSession session = prestoSparkSessionFactory.createSession(clientOptions);
        IPrestoSparkExecution execution = executionFactory.create(sparkContext, session, query, new DistributionPrestoSparkTaskCompilerFactory(serviceFactory));

        List<List<Object>> results = execution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);

        try {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static PrestoSparkDistribution createDistribution(PrestoSparkClientOptions clientOptions)
    {
        return new PrestoSparkDistribution(
                new File(clientOptions.packagePath),
                new File(clientOptions.config),
                new File(clientOptions.catalogs));
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
        // TODO
        return new PrestoSparkConfiguration(
                distribution.getLocalConfigFile().getAbsolutePath(),
                new File(distribution.getLocalPackageDirectory(), "plugin").getAbsolutePath(),
                distribution.getLocalCatalogsDirectory().getAbsolutePath(),
                ImmutableMap.of());
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

    public static class PrestoSparkDistribution
            implements Serializable
    {
        private static final String CATALOGS_ARCHIVE_NAME = "catalogs.tar.gz";

        private final File packageFile;
        private final File config;
        private final File catalogs;
        private final String fingerprint;

        public PrestoSparkDistribution(File packageFile, File config, File catalogs)
        {
            this.packageFile = checkFile(requireNonNull(packageFile, "packageFile is null"));
            checkFile(config);
            checkArgument(!config.getName().equals(packageFile.getName()), "package file name and config file name must be different: %s", config.getName());
            this.config = requireNonNull(config, "config is null");
            checkDirectory(catalogs);
            checkArgument(!catalogs.getName().equals(packageFile.getName()), "catalogs directory name and package file name must be different: %s", catalogs.getName());
            checkArgument(!catalogs.getName().equals(config.getName()), "catalogs directory name and config file name must be different: %s", catalogs.getName());
            this.catalogs = requireNonNull(catalogs, "catalogs is null");
            this.fingerprint = computeFingerprint(packageFile, config, catalogs);
        }

        private static String computeFingerprint(File packageFile, File config, File catalogs)
        {
            try {
                ImmutableList.Builder<HashCode> hashes = ImmutableList.builder();
                hashes.add(asByteSource(packageFile).hash(Hashing.sha256()));
                hashes.add(asByteSource(config).hash(Hashing.sha256()));
                File[] catalogConfigurations = catalogs.listFiles();
                sort(catalogConfigurations);
                for (File catalogConfiguration : catalogConfigurations) {
                    hashes.add(Hashing.sha256().hashString(catalogConfiguration.getName(), UTF_8));
                    hashes.add(asByteSource(catalogConfiguration).hash(Hashing.sha256()));
                }
                return combineOrdered(hashes.build()).toString();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void deploy(SparkContext context, File tempDir)
        {
            context.addFile(packageFile.getAbsolutePath());
            context.addFile(config.getAbsolutePath());

            File catalogsArchive = new File(tempDir, CATALOGS_ARCHIVE_NAME);
            TarGz.create(catalogs, catalogsArchive);
            context.addFile(catalogsArchive.getAbsolutePath());
        }

        public File getLocalPackageDirectory()
        {
            File localPackageFile = packageFile.exists() ? packageFile : getLocalFile(packageFile.getName());
            return ensureDecompressed(localPackageFile, new File(SparkFiles.getRootDirectory()));
        }

        public File getLocalConfigFile()
        {
            if (config.exists()) {
                return config;
            }
            return getLocalFile(config.getName());
        }

        public File getLocalCatalogsDirectory()
        {
            if (catalogs.exists()) {
                return catalogs;
            }
            return ensureDecompressed(getLocalFile(CATALOGS_ARCHIVE_NAME), new File(SparkFiles.getRootDirectory()));
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

    public static class DistributionPrestoSparkTaskCompilerFactory
            implements PrestoSparkTaskCompilerFactory
    {
        private final CachingServiceFactory serviceFactory;

        public DistributionPrestoSparkTaskCompilerFactory(CachingServiceFactory serviceFactory)
        {
            this.serviceFactory = requireNonNull(serviceFactory, "serviceFactory is null");
        }

        @Override
        public IPrestoSparkTaskCompiler create()
        {
            return serviceFactory.createService().createTaskCompiler();
        }
    }
}
