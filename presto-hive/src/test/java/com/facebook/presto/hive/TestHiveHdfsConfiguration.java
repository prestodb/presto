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
package com.facebook.presto.hive;

import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Hadoop's {@link Configuration} captures {@code Thread.currentThread().getContextClassLoader()} in
 * an instance initializer, so the ClassLoader baked into the {@link ThreadLocal} maintained by
 * {@link HiveHdfsConfiguration} depends on whichever thread first populated it. Executor threads
 * carry the plugin ClassLoader, but query planning threads carry the system ClassLoader, which
 * cannot see plugin classes such as {@code PrestoS3FileSystem} (registered as {@code fs.s3.impl}).
 * Resolving the filesystem implementation from a planning thread then fails with
 * {@code ClassNotFoundException}.
 * <p>
 * These tests verify the plugin ClassLoader is pinned on every Configuration handed out, across all
 * three return paths of {@link HiveHdfsConfiguration#getConfiguration}.
 */
public class TestHiveHdfsConfiguration
{
    private static final String PLUGIN_CLASS = "com.facebook.presto.hive.s3.PrestoS3FileSystem";
    private static final HdfsContext CONTEXT = new HdfsContext(new ConnectorIdentity("user", Optional.empty(), Optional.empty()));
    private static final URI URI_S3 = URI.create("s3://bucket/path");

    @Test
    public void testClassLoaderPinnedWithoutDynamicProviders()
            throws Exception
    {
        // No dynamic providers: getConfiguration() returns the ThreadLocal instance directly.
        Configuration configuration = configurationFromRestrictedThread(hdfsConfiguration(false, ImmutableSet.of()));

        assertPluginClassLoader(configuration);
    }

    @Test
    public void testClassLoaderPinnedWithDynamicProviders()
            throws Exception
    {
        // A URI-dependent provider forces the copy-constructor path,
        // new Configuration(hadoopConfiguration.get()). Hadoop's copy constructor propagates
        // classLoader, so the pin has to survive it.
        Configuration configuration = configurationFromRestrictedThread(
                hdfsConfiguration(false, ImmutableSet.of(new TestingDynamicConfigurationProvider(false))));

        assertPluginClassLoader(configuration);
    }

    @Test
    public void testClassLoaderPinnedWithCopyOnFirstWriteConfiguration()
            throws Exception
    {
        // A URI-independent provider plus hive.copy-on-first-write-configuration-enabled returns a
        // CopyOnFirstWriteConfiguration wrapper, which delegates getClassLoader() and
        // getClassByName() to the configuration it wraps.
        HiveHdfsConfiguration hdfsConfiguration = hdfsConfiguration(true, ImmutableSet.of(new TestingDynamicConfigurationProvider(true)));
        Configuration configuration = configurationFromRestrictedThread(hdfsConfiguration);

        assertTrue(configuration instanceof CopyOnFirstWriteConfiguration, "expected a CopyOnFirstWriteConfiguration, got " + configuration.getClass().getName());
        assertPluginClassLoader(configuration);

        // Pinning must not write through the wrapper: CopyOnFirstWriteConfiguration.setClassLoader()
        // triggers copy-on-write, which would deep copy the shared configuration on every
        // getConfiguration() call and defeat the point of the flag. If no copy was triggered, both
        // wrappers still share the cached uriAgnosticConfiguration instance.
        Configuration second = hdfsConfiguration.getConfiguration(CONTEXT, URI_S3);
        assertSame(
                ((CopyOnFirstWriteConfiguration) second).getConfig(),
                ((CopyOnFirstWriteConfiguration) configuration).getConfig(),
                "copy-on-write was triggered while pinning the ClassLoader");
    }

    @Test
    public void testClassLoaderPinnedAfterConfigurationCopy()
            throws Exception
    {
        // ConfigurationUtils.copy() builds a new Configuration and carries over key/value entries,
        // which does not include the ClassLoader. Callers such as
        // HiveUtil.initializeDeserializer() copy a configuration they were handed, so the pin has
        // to be carried over explicitly.
        Configuration configuration = hdfsConfiguration(false, ImmutableSet.of()).getConfiguration(CONTEXT, URI_S3);
        Configuration copied = onRestrictedThread(() -> copy(configuration));

        assertPluginClassLoader(copied);
    }

    private static HiveHdfsConfiguration hdfsConfiguration(boolean copyOnFirstWriteEnabled, Set<DynamicConfigurationProvider> dynamicProviders)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig()
                .setCopyOnFirstWriteConfigurationEnabled(copyOnFirstWriteEnabled);
        return new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, new MetastoreClientConfig()),
                dynamicProviders,
                hiveClientConfig);
    }

    private static Configuration configurationFromRestrictedThread(HdfsConfiguration hdfsConfiguration)
            throws Exception
    {
        return onRestrictedThread(() -> hdfsConfiguration.getConfiguration(CONTEXT, URI_S3));
    }

    /**
     * Runs {@code supplier} on a thread whose context ClassLoader cannot see plugin classes and
     * returns the configuration it produced. In production that thread is a query planning thread
     * carrying the system ClassLoader; an empty {@link URLClassLoader} with a null parent gives
     * the same isolation here.
     */
    private static Configuration onRestrictedThread(Supplier<Configuration> supplier)
            throws Exception
    {
        AtomicReference<Configuration> configurationReference = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread thread = new Thread(() -> {
            try {
                configurationReference.set(supplier.get());
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

        if (failure.get() != null) {
            throw new AssertionError("action failed on the restricted thread", failure.get());
        }
        return configurationReference.get();
    }

    private static void assertPluginClassLoader(Configuration configuration)
            throws ClassNotFoundException
    {
        assertNotNull(configuration);
        assertSame(
                configuration.getClassLoader(),
                HiveHdfsConfiguration.class.getClassLoader(),
                "Configuration must carry the plugin ClassLoader, not the calling thread's ClassLoader");
        // Reproduce the production failure path: Hadoop resolves the filesystem implementation
        // through Configuration.getClassByName(), which uses the pinned ClassLoader.
        assertNotNull(configuration.getClassByName(PLUGIN_CLASS));
    }

    private static class TestingDynamicConfigurationProvider
            implements DynamicConfigurationProvider
    {
        private final boolean uriIndependent;

        TestingDynamicConfigurationProvider(boolean uriIndependent)
        {
            this.uriIndependent = uriIndependent;
        }

        @Override
        public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
        {
            configuration.set("testing.dynamic.property", "value");
        }

        @Override
        public boolean isUriIndependentConfigurationProvider()
        {
            return uriIndependent;
        }
    }
}
