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
package com.facebook.presto.spark.testing;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestDockerCompose
{
    private File composeYaml;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        composeYaml = Files.createTempFile("docker-compose", ".yml").toFile();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (composeYaml != null) {
            Files.deleteIfExists(composeYaml.toPath());
        }
    }

    @Test(timeOut = 10_000)
    public void testWaitsForContainerStartup()
            throws InterruptedException
    {
        AtomicInteger attempts = new AtomicInteger();
        DockerCompose compose = new TestingDockerCompose(service -> {
            assertEquals(service, "hadoop-master");
            return attempts.incrementAndGet() < 3 ? Optional.empty() : Optional.of("container-id");
        });

        assertEquals(compose.getContainerId("hadoop-master", Duration.ofSeconds(5)), "container-id");
        assertEquals(attempts.get(), 3);
    }

    @Test
    public void testAlreadyRunningContainer()
            throws InterruptedException
    {
        DockerCompose compose = new TestingDockerCompose(service -> Optional.of("container-id"));
        assertEquals(compose.getContainerId("hadoop-master", Duration.ZERO), "container-id");
    }

    @Test
    public void testMissingContainerTimesOut()
    {
        AtomicInteger attempts = new AtomicInteger();
        DockerCompose compose = new TestingDockerCompose(service -> {
            attempts.incrementAndGet();
            return Optional.empty();
        });

        assertThrows(IllegalArgumentException.class, () -> compose.getContainerId("hadoop-master", Duration.ZERO));
        assertEquals(attempts.get(), 1);
    }

    @Test
    public void testInterruptedWait()
    {
        DockerCompose compose = new TestingDockerCompose(service -> Optional.empty());
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> compose.getContainerId("hadoop-master", Duration.ofSeconds(5)));
        }
        finally {
            Thread.interrupted();
        }
    }

    private class TestingDockerCompose
            extends DockerCompose
    {
        private final Function<String, Optional<String>> containerLookup;

        private TestingDockerCompose(Function<String, Optional<String>> containerLookup)
        {
            super(composeYaml);
            this.containerLookup = containerLookup;
        }

        @Override
        Optional<String> findContainerId(String service)
        {
            return containerLookup.apply(service);
        }
    }
}
