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
package com.facebook.presto.hive.cache;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.NoOpCacheManager;
import com.facebook.presto.hadoop.FileSystemFactory;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

/**
 * The plugin ClassLoader is pinned in {@link HiveHdfsConfiguration}, which is where every
 * Configuration originates. These tests verify the caching layer preserves it: the
 * {@code CachingJobConf} wrapper must delegate class resolution to the pinned configuration, and the
 * branch that fetches a fresh configuration for a different URI must be pinned too.
 */
public class TestHiveCachingHdfsConfiguration
{
    private static final HdfsContext CONTEXT = new HdfsContext(new ConnectorIdentity("user", Optional.empty(), Optional.empty()));
    private static final URI URI_S3 = URI.create("s3://bucket/path");
    private static final URI OTHER_URI_S3 = URI.create("s3://other-bucket/other-path");

    /**
     * Hadoop loads the filesystem implementations it knows about once per JVM, through the context
     * ClassLoader of whichever thread asks first, and keeps the result for the life of the JVM. One
     * of the tests below asks from a thread whose ClassLoader deliberately sees nothing, which
     * would leave that registry empty and break every filesystem lookup in this JVM afterwards,
     * including those of unrelated tests sharing it. Asking here first, from a thread that can see
     * them, fills the registry with the real implementations.
     */
    @BeforeClass
    public void loadHadoopFileSystemImplementations()
            throws IOException
    {
        FileSystem.get(URI.create("file:///"), new Configuration());
    }

    @Test
    public void testClassLoaderSurvivesCachingWrapper()
            throws Exception
    {
        RecordingHdfsConfiguration delegate = new RecordingHdfsConfiguration(hiveHdfsConfiguration());
        Configuration configuration = configurationFromRestrictedThread(cachingHdfsConfiguration(delegate));

        // CachingJobConf wraps the pinned configuration and delegates getClassLoader() and
        // getClassByName() to it.
        assertPluginClassLoader(configuration);
    }

    @Test
    public void testClassLoaderPinnedForDifferentUri()
            throws Exception
    {
        // CachingJobConf.createFileSystem() reuses the configuration it was built with only when the
        // URIs match; for any other URI it fetches a fresh one from the delegate. Both must carry the
        // plugin ClassLoader, otherwise resolving "fs.s3.impl" from a planning thread fails.
        RecordingHdfsConfiguration delegate = new RecordingHdfsConfiguration(hiveHdfsConfiguration());
        Configuration configuration = configurationFromRestrictedThread(cachingHdfsConfiguration(delegate));

        delegate.clear();
        createFileSystemFromRestrictedThread(configuration, OTHER_URI_S3);

        List<Configuration> fetched = delegate.getRecorded();
        assertFalse(fetched.isEmpty(), "createFileSystem() did not fetch a configuration for the other URI");
        for (Configuration fresh : fetched) {
            assertPluginClassLoader(fresh);
        }
    }

    @Test
    public void testFileSystemImplementationResolvesFromRestrictedThread()
            throws Exception
    {
        // End-to-end version of the reported failure: with "fs.s3.impl" registered, Hadoop resolves
        // the implementation class through Configuration.getClass(), which uses the pinned
        // ClassLoader rather than the calling thread's.
        Configuration configuration = configurationFromRestrictedThread(cachingHdfsConfiguration(hiveHdfsConfigurationWithS3()));

        assertEquals(configuration.get("fs.s3.impl"), PrestoS3FileSystem.class.getName());
        assertEquals(configuration.getClass("fs.s3.impl", null), PrestoS3FileSystem.class);
    }

    private static HiveHdfsConfiguration hiveHdfsConfiguration()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        return new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, new MetastoreClientConfig()),
                ImmutableSet.of(),
                hiveClientConfig);
    }

    private static HiveHdfsConfiguration hiveHdfsConfigurationWithS3()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        return new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        hiveClientConfig,
                        new MetastoreClientConfig(),
                        new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                        ignored -> {},
                        ignored -> {}),
                ImmutableSet.of(),
                hiveClientConfig);
    }

    private static HiveCachingHdfsConfiguration cachingHdfsConfiguration(HdfsConfiguration delegate)
    {
        return new HiveCachingHdfsConfiguration(
                delegate,
                new CacheConfig(),
                new NoOpCacheManager(),
                new CacheFactory());
    }

    /**
     * Calls {@code getConfiguration} on a thread whose context ClassLoader cannot see plugin
     * classes. In production this is the system ClassLoader on a query planning thread; an empty
     * {@link URLClassLoader} with a null parent gives the same isolation here.
     */
    private static Configuration configurationFromRestrictedThread(HdfsConfiguration hdfsConfiguration)
            throws Exception
    {
        AtomicReference<Configuration> result = new AtomicReference<>();
        runOnRestrictedThread(() -> result.set(hdfsConfiguration.getConfiguration(CONTEXT, URI_S3)), true);
        return result.get();
    }

    /**
     * Drives {@code CachingJobConf.createFileSystem()} from a restricted thread. Creating the
     * filesystem itself is expected to fail in a unit test - there is no S3 endpoint and the
     * configured implementation is not an {@code ExtendedFileSystem} - so failures are ignored. What
     * matters is the configuration the caching layer picked up on the way, which the recording
     * delegate captures.
     */
    private static void createFileSystemFromRestrictedThread(Configuration configuration, URI uri)
            throws Exception
    {
        FileSystemFactory factory = (FileSystemFactory) configuration;
        runOnRestrictedThread(() -> factory.createFileSystem(uri), false);
    }

    private static void runOnRestrictedThread(Runnable action, boolean propagateFailure)
            throws Exception
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                action.run();
            }
            catch (Throwable t) {
                failure.set(t);
            }
        });
        try (URLClassLoader restrictedClassLoader = new URLClassLoader(new URL[0], null)) {
            thread.setContextClassLoader(restrictedClassLoader);
            thread.start();
            thread.join();
        }

        if (propagateFailure && failure.get() != null) {
            throw new AssertionError("action failed on the restricted thread", failure.get());
        }
    }

    private static void assertPluginClassLoader(Configuration configuration)
            throws ClassNotFoundException
    {
        assertNotNull(configuration);
        assertSame(
                configuration.getClassLoader(),
                HiveHdfsConfiguration.class.getClassLoader(),
                "Configuration must carry the plugin ClassLoader, not the calling thread's ClassLoader");
        assertNotNull(configuration.getClassByName(PrestoS3FileSystem.class.getName()));
    }

    private static class RecordingHdfsConfiguration
            implements HdfsConfiguration
    {
        private final HdfsConfiguration delegate;
        private final List<Configuration> recorded = new CopyOnWriteArrayList<>();

        RecordingHdfsConfiguration(HdfsConfiguration delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public Configuration getConfiguration(HdfsContext context, URI uri)
        {
            Configuration configuration = delegate.getConfiguration(context, uri);
            recorded.add(configuration);
            return configuration;
        }

        List<Configuration> getRecorded()
        {
            return ImmutableList.copyOf(recorded);
        }

        void clear()
        {
            recorded.clear();
        }
    }
}
