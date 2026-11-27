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

import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkBootstrapTimer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.facebook.presto.spark.launcher.PrestoSparkRunner.CreatedService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static com.facebook.presto.spark.launcher.PrestoSparkRunner.createServiceFactory;
import static com.facebook.presto.spark.launcher.PrestoSparkRunner.startService;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPrestoSparkRunner
{
    private static final String SERVICES_FILE = "META-INF/services/" + IPrestoSparkServiceFactory.class.getName();

    @Test
    public void testCreateServiceFactoryFallsBackToTheFullClasspath()
            throws IOException
    {
        // A lib/ directory like a package's, built from this test's own classes: the provider in
        // one jar, its superclass in another, and a jar the provider does not need.
        Path packageDirectory = Files.createTempDirectory("package");
        Path lib = Files.createDirectory(packageDirectory.resolve("lib"));
        writeJar(lib.resolve("provider.jar"), ImmutableMap.of(
                classFile(Provider.class), classBytes(Provider.class),
                SERVICES_FILE, (Provider.class.getName() + "\n").getBytes(UTF_8)));
        writeJar(lib.resolve("provider-base.jar"), ImmutableMap.of(
                classFile(ProviderBase.class), classBytes(ProviderBase.class)));
        writeJar(lib.resolve("unused.jar"), ImmutableMap.of("unused.txt", new byte[0]));
        try {
            // no narrowed copy: the full classpath
            assertLoaded(createServiceFactory(lib.toFile(), Optional.empty()), false);

            // a copy without a jar the provider does not need: the narrowed classpath
            assertLoaded(createServiceFactory(lib.toFile(), Optional.of(narrowedLib(packageDirectory, "unused.jar"))), true);

            // a copy without the jar holding the class the provider extends: ServiceLoader throws a
            // bare NoClassDefFoundError, and the full classpath is used instead of failing to start
            assertLoaded(createServiceFactory(lib.toFile(), Optional.of(narrowedLib(packageDirectory, "provider-base.jar"))), false);

            // a copy without the provider's own jar: no provider is found, and the full classpath is used
            assertLoaded(createServiceFactory(lib.toFile(), Optional.of(narrowedLib(packageDirectory, "provider.jar"))), false);

            // a copy holding something other than links: the full classpath is used instead of failing to start
            File notLinks = narrowedLib(packageDirectory, "unused.jar");
            Files.createDirectory(notLinks.toPath().resolve("directory"));
            assertLoaded(createServiceFactory(lib.toFile(), Optional.of(notLinks)), false);
        }
        finally {
            MoreFiles.deleteRecursively(packageDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    /** Creates a new lib-native in {@code packageDirectory}: a relative link to every lib/ file but {@code excludedJar}. */
    private static File narrowedLib(Path packageDirectory, String excludedJar)
            throws IOException
    {
        Path narrowed = packageDirectory.resolve(NarrowedExecutorClasspath.LIB_DIRECTORY);
        if (Files.exists(narrowed)) {
            MoreFiles.deleteRecursively(narrowed, RecursiveDeleteOption.ALLOW_INSECURE);
        }
        Files.createDirectory(narrowed);
        try (DirectoryStream<Path> files = Files.newDirectoryStream(packageDirectory.resolve("lib"))) {
            for (Path file : files) {
                if (!file.getFileName().toString().equals(excludedJar)) {
                    Files.createSymbolicLink(narrowed.resolve(file.getFileName()), Paths.get("../lib").resolve(file.getFileName()));
                }
            }
        }
        return narrowed.toFile();
    }

    @Test
    public void testStartServiceRetriesOnTheFullClasspath()
    {
        IPrestoSparkService narrowedService = new StubService();
        IPrestoSparkService fullService = new StubService();
        AtomicInteger fullAttempts = new AtomicInteger();
        AtomicInteger retryFailures = new AtomicInteger();
        Supplier<IPrestoSparkService> full = () -> {
            fullAttempts.incrementAndGet();
            return fullService;
        };

        // the narrowed classpath works: no retry, and it counts as narrowed
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CreatedService created = startService(true, () -> narrowedService, full, () -> false, retryFailures::incrementAndGet, new PrintStream(err, true));
        assertSame(created.service, narrowedService);
        assertTrue(created.classpathNarrowed);
        assertEquals(fullAttempts.get(), 0);
        assertEquals(err.size(), 0);

        // a class missing from the narrowed classpath -- thrown bare, wrapped by Guice or by the
        // injector factory, or as a service configuration error: retried on the full
        // classpath, which no longer counts as narrowed, and the cause goes to stderr
        List<Throwable> failures = ImmutableList.of(
                new NoClassDefFoundError("com/mysql/cj/jdbc/NonRegisteringDriver"),
                new RuntimeException(new NoClassDefFoundError("org/antlr/runtime/Lexer")),
                new ServiceConfigurationError("com.facebook.presto.spi.Plugin: Provider not found"));
        for (Throwable failure : failures) {
            fullAttempts.set(0);
            err.reset();
            created = startService(true, () -> throwUnchecked(failure), full, () -> false, retryFailures::incrementAndGet, new PrintStream(err, true));
            assertSame(created.service, fullService);
            assertFalse(created.classpathNarrowed);
            assertEquals(fullAttempts.get(), 1);
            assertTrue(err.toString().contains("failed to start on the classpath narrowed by presto.spark.narrowed-executor-classpath-enabled; "
                    + "retrying once with the full classpath"), err.toString());
            assertTrue(err.toString().contains(failure.toString()), err.toString());
        }
        // a retry that succeeds leaves later tasks free to narrow
        assertEquals(retryFailures.get(), 0);
    }

    @Test
    public void testStartServiceFailures()
    {
        AtomicInteger fullAttempts = new AtomicInteger();
        AtomicInteger retryFailures = new AtomicInteger();
        Supplier<IPrestoSparkService> full = () -> {
            fullAttempts.incrementAndGet();
            return new StubService();
        };
        PrintStream err = new PrintStream(new ByteArrayOutputStream(), true);

        // nothing was narrowed: the failure is thrown as is, without a retry
        IllegalStateException notNarrowed = new IllegalStateException("bad catalog property");
        assertSame(expectThrows(IllegalStateException.class, () -> startService(false, () -> throwUnchecked(notNarrowed), full, () -> false, retryFailures::incrementAndGet, err)), notNarrowed);
        assertEquals(fullAttempts.get(), 0);

        // the retry fails too, e.g. for a cause the narrowing did not cause: its failure is thrown,
        // with the first attempt's suppressed in it
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException retry = new IllegalStateException("retry");
        IllegalStateException thrown = expectThrows(IllegalStateException.class,
                () -> startService(true, () -> throwUnchecked(first), () -> throwUnchecked(retry), () -> false, retryFailures::incrementAndGet, err));
        assertSame(thrown, retry);
        assertEquals(ImmutableList.copyOf(thrown.getSuppressed()), ImmutableList.of(first));
        // ... and only then is the caller told to start later services on the full classpath only
        assertEquals(retryFailures.get(), 1);

        // the JVM itself failing is not retried
        fullAttempts.set(0);
        OutOfMemoryError oom = new OutOfMemoryError("Java heap space");
        assertSame(expectThrows(OutOfMemoryError.class, () -> startService(true, () -> throwUnchecked(oom), full, () -> false, retryFailures::incrementAndGet, err)), oom);
        assertEquals(fullAttempts.get(), 0);

        // the task is being killed (here, a speculative attempt whose twin succeeded, interrupted
        // while Configerator was connecting): thrown as is, and not retried
        IllegalStateException killed = new IllegalStateException("Unable to initiate session during class initialization");
        assertSame(expectThrows(IllegalStateException.class, () -> startService(true, () -> throwUnchecked(killed), full, () -> true, retryFailures::incrementAndGet, err)), killed);
        assertEquals(fullAttempts.get(), 0);
        // none of the others retried and failed: a killed task must not turn narrowing off for good
        assertEquals(retryFailures.get(), 1);

        // the task is killed while the retry runs (here, the first attempt hit a read error on a
        // jar): the retry's failure is thrown, but narrowing stays on for later tasks
        AtomicBoolean killedDuringRetry = new AtomicBoolean();
        IllegalStateException interruptedRetry = new IllegalStateException("loadConfigs execution was interrupted");
        assertSame(expectThrows(IllegalStateException.class, () -> startService(true, () -> throwUnchecked(new NoClassDefFoundError("AbstractConstraintMetaData")), () -> {
            killedDuringRetry.set(true);
            throw interruptedRetry;
        }, killedDuringRetry::get, retryFailures::incrementAndGet, err)), interruptedRetry);
        assertEquals(retryFailures.get(), 1);
    }

    private static IPrestoSparkService throwUnchecked(Throwable failure)
    {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw (Error) failure;
    }

    private static class StubService
            implements IPrestoSparkService
    {
        @Override
        public IPrestoSparkQueryExecutionFactory getQueryExecutionFactory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory getTaskExecutorFactory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPrestoSparkTaskExecutorFactory getNativeTaskExecutorFactory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
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
