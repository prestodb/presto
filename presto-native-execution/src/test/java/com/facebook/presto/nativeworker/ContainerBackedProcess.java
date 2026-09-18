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
package com.facebook.presto.nativeworker;

import org.testcontainers.containers.GenericContainer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Adapts a Testcontainers {@link GenericContainer} to the {@link Process} interface,
 * allowing containerized native workers to be managed by
 * {@link com.facebook.presto.tests.DistributedQueryRunner} which expects
 * external workers as {@link Process} instances.
 * <p>
 * This adapter enables the existing external worker launcher mechanism
 * ({@code BiFunction<Integer, URI, Process>}) to work with container-based
 * workers without modifying {@code DistributedQueryRunner}.
 * <p>
 * Log output is captured via Testcontainers log consumers configured on the
 * container before starting, not via {@link #getInputStream()} or
 * {@link #getErrorStream()}, which return empty streams.
 */
class ContainerBackedProcess
        extends Process
{
    private final GenericContainer<?> container;

    ContainerBackedProcess(GenericContainer<?> container)
    {
        this.container = container;
    }

    GenericContainer<?> getContainer()
    {
        return container;
    }

    @Override
    public OutputStream getOutputStream()
    {
        // Container processes don't support stdin from the host
        return new OutputStream()
        {
            @Override
            public void write(int b)
            {
                // no-op
            }
        };
    }

    @Override
    public InputStream getInputStream()
    {
        // Logs are captured via Testcontainers log consumers, not via process streams
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public InputStream getErrorStream()
    {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public int waitFor()
            throws InterruptedException
    {
        while (container.isRunning()) {
            Thread.sleep(500);
        }
        return 0;
    }

    @Override
    public int exitValue()
    {
        if (container.isRunning()) {
            throw new IllegalThreadStateException("Container is still running");
        }
        return 0;
    }

    @Override
    public void destroy()
    {
        if (container.isRunning()) {
            container.stop();
        }
    }

    @Override
    public Process destroyForcibly()
    {
        if (container.isRunning()) {
            container.stop();
        }
        return this;
    }

    @Override
    public boolean isAlive()
    {
        return container.isRunning();
    }
}
