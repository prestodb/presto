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
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestHiveCachingHdfsConfiguration
{
    /**
     * Regression test for ClassNotFoundException when IcebergFilterPushdown accesses Iceberg
     * table metadata during query planning on the main Presto planning thread.
     *
     * The planning thread's context ClassLoader is the System ClassLoader, which does not have
     * plugin classes such as PrestoS3FileSystem. HiveCachingHdfsConfiguration.getConfiguration()
     * must pin the plugin ClassLoader on the returned Configuration so that
     * conf.getClassByName("com.facebook.presto.hive.s3.PrestoS3FileSystem") — called internally
     * by Hadoop when creating the S3 FileSystem — succeeds regardless of the calling thread.
     */
    @Test
    public void testConfigurationClassLoaderPinnedToPluginClassLoader()
            throws Exception
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        HiveCachingHdfsConfiguration cachingConfig = new HiveCachingHdfsConfiguration(
                new HiveHdfsConfiguration(
                        new HdfsConfigurationInitializer(hiveClientConfig, new MetastoreClientConfig()),
                        ImmutableSet.of(),
                        hiveClientConfig),
                new CacheConfig(),
                new NoOpCacheManager(),
                new CacheFactory());

        HdfsContext context = new HdfsContext(new ConnectorIdentity("user", Optional.empty(), Optional.empty()));
        URI uri = URI.create("s3://bucket/path");

        // Simulate the planning thread: give it a ClassLoader that cannot see any plugin classes.
        // In production this is the JVM System ClassLoader; here we use an empty URLClassLoader
        // with a null parent to achieve the same isolation.
        ClassLoader planningThreadClassLoader = new URLClassLoader(new URL[0], null);

        AtomicReference<Configuration> configRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Thread planningThread = new Thread(() -> {
            try {
                configRef.set(cachingConfig.getConfiguration(context, uri));
            }
            catch (Throwable t) {
                errorRef.set(t);
            }
        });
        planningThread.setContextClassLoader(planningThreadClassLoader);
        planningThread.start();
        planningThread.join();

        if (errorRef.get() != null) {
            fail("getConfiguration() threw unexpectedly: " + errorRef.get());
        }

        Configuration conf = configRef.get();
        assertNotNull(conf);

        // The Configuration's ClassLoader must be the plugin ClassLoader (the one that loaded
        // HiveCachingHdfsConfiguration), NOT the restricted planning-thread ClassLoader.
        assertSame(
                conf.getClassLoader(),
                HiveCachingHdfsConfiguration.class.getClassLoader(),
                "Configuration ClassLoader must be the plugin ClassLoader, not the calling thread's ClassLoader");

        // Directly reproduce the production failure path: Hadoop calls conf.getClassByName()
        // to resolve the filesystem implementation set by PrestoS3ConfigurationUpdater
        // (config.set("fs.s3.impl", PrestoS3FileSystem.class.getName())).
        // This must succeed even though we are on a thread with a restricted ClassLoader.
        try {
            conf.getClassByName("com.facebook.presto.hive.s3.PrestoS3FileSystem");
        }
        catch (ClassNotFoundException e) {
            fail("conf.getClassByName(PrestoS3FileSystem) threw ClassNotFoundException; " +
                    "the plugin ClassLoader was not pinned on the Configuration. " +
                    "This reproduces the planning-thread bug: " + e.getMessage());
        }
    }

    /**
     * Without the fix, calling getConfiguration() from a thread with a restricted ClassLoader
     * leaves the Configuration's ClassLoader pointing at that restricted ClassLoader.
     * This test documents and verifies the old (broken) behavior to show what the fix prevents.
     */
    @Test
    public void testWithoutFixConfigurationInheritsCallingThreadClassLoader()
            throws Exception
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        HiveHdfsConfiguration innerConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, new MetastoreClientConfig()),
                ImmutableSet.of(),
                hiveClientConfig);

        HdfsContext context = new HdfsContext(new ConnectorIdentity("user", Optional.empty(), Optional.empty()));
        URI uri = URI.create("s3://bucket/path");

        // Without the fix, the inner Configuration's classLoader comes from the thread that
        // calls innerConfig.getConfiguration(). We verify that here directly:
        ClassLoader restrictedClassLoader = new URLClassLoader(new URL[0], null);
        AtomicReference<Configuration> innerConfigRef = new AtomicReference<>();

        Thread thread = new Thread(() -> innerConfigRef.set(innerConfig.getConfiguration(context, uri)));
        thread.setContextClassLoader(restrictedClassLoader);
        thread.start();
        thread.join();

        Configuration rawConf = innerConfigRef.get();
        assertNotNull(rawConf);
        // The inner Configuration inherits the restricted ClassLoader from the calling thread.
        // This is why the fix must call defaultConfig.setClassLoader(getClass().getClassLoader())
        // — to override this thread-inherited ClassLoader with the stable plugin ClassLoader.
        assertSame(
                rawConf.getClassLoader(),
                restrictedClassLoader,
                "Without the fix, the inner Configuration inherits the calling thread's ClassLoader");
    }
}
