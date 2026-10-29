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

import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkBootstrapTimer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static com.facebook.presto.spark.launcher.PrestoSparkRunner.createServiceFactory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

public class TestPrestoSparkRunner
{
    private static final String SERVICES_FILE = "META-INF/services/" + IPrestoSparkServiceFactory.class.getName();

    @Test
    public void testCreateServiceFactoryFallsBackToTheFullClasspath()
            throws IOException
    {
        // A lib/ directory like a package's, built from this test's own classes: the provider in
        // one jar, its superclass in another, and a jar the provider does not need.
        Path lib = Files.createTempDirectory("lib");
        List<Path> jars = ImmutableList.of(
                writeJar(lib.resolve("provider.jar"), ImmutableMap.of(
                        classFile(Provider.class), classBytes(Provider.class),
                        SERVICES_FILE, (Provider.class.getName() + "\n").getBytes(UTF_8))),
                writeJar(lib.resolve("provider-base.jar"), ImmutableMap.of(
                        classFile(ProviderBase.class), classBytes(ProviderBase.class))),
                writeJar(lib.resolve("unused.jar"), ImmutableMap.of("unused.txt", new byte[0])));
        try {
            // nothing excluded: the full classpath
            assertLoaded(createServiceFactory(lib.toFile(), name -> false), false);

            // a jar the provider does not need: the narrowed classpath
            assertLoaded(createServiceFactory(lib.toFile(), "unused.jar"::equals), true);

            // the jar holding the class the provider extends: ServiceLoader throws a bare
            // NoClassDefFoundError, and the full classpath is used instead of failing to start
            assertLoaded(createServiceFactory(lib.toFile(), "provider-base.jar"::equals), false);

            // the provider's own jar: no provider is found, and the full classpath is used
            assertLoaded(createServiceFactory(lib.toFile(), "provider.jar"::equals), false);
        }
        finally {
            for (Path jar : jars) {
                Files.delete(jar);
            }
            Files.delete(lib);
        }
    }

    private static void assertLoaded(PrestoSparkRunner.LoadedServiceFactory loaded, boolean narrowed)
    {
        assertEquals(loaded.factory.getClass().getName(), Provider.class.getName());
        // loaded from the jars, not from this test's classpath
        assertNotSame(loaded.factory.getClass(), Provider.class);
        assertEquals(loaded.narrowed, narrowed);
    }

    private static Path writeJar(Path jar, Map<String, byte[]> entries)
            throws IOException
    {
        try (OutputStream file = Files.newOutputStream(jar); JarOutputStream out = new JarOutputStream(file)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                out.putNextEntry(new JarEntry(entry.getKey()));
                out.write(entry.getValue());
                out.closeEntry();
            }
        }
        return jar;
    }

    private static String classFile(Class<?> clazz)
    {
        return clazz.getName().replace('.', '/') + ".class";
    }

    private static byte[] classBytes(Class<?> clazz)
            throws IOException
    {
        return Resources.toByteArray(Resources.getResource(classFile(clazz)));
    }

    public abstract static class ProviderBase
            implements IPrestoSparkServiceFactory
    {
        @Override
        public IPrestoSparkService createService(SparkProcessType sparkProcessType, PrestoSparkConfiguration configuration, PrestoSparkBootstrapTimer bootstrapTimer)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class Provider
            extends ProviderBase
    {
    }
}
