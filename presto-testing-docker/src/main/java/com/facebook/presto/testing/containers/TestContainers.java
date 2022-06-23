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
package com.facebook.presto.testing.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;

public final class TestContainers
{
    // To reuse container set TESTCONTAINERS_REUSE_ENABLE=true environment variable.
    // TESTCONTAINERS_REUSE_ENABLE is an environment variable defined in testcontainers library.
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = parseBoolean(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    private TestContainers() {}

    // You should not close the container directly if you want to reuse it.
    // Instead you should close closeable returned by {@link this::startOrReuse}
    public static Closeable startOrReuse(GenericContainer<?> container)
    {
        boolean reuse = TestcontainersConfiguration.getInstance().environmentSupportsReuse();
        checkState(reuse == TESTCONTAINERS_REUSE_ENABLE, "Unable to enable or disable container reuse");

        container.withReuse(TESTCONTAINERS_REUSE_ENABLE);
        container.start();
        if (reuse) {
            return () -> {};
        }
        return container::stop;
    }
}
