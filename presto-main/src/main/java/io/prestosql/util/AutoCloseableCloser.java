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
package io.prestosql.util;

import java.util.ArrayDeque;
import java.util.Deque;

import static com.google.common.base.Throwables.propagateIfPossible;
import static java.util.Objects.requireNonNull;

/**
 * This class is inspired by com.google.common.io.Closer
 */
public final class AutoCloseableCloser
        implements AutoCloseable
{
    private final Deque<AutoCloseable> stack = new ArrayDeque<>(4);

    private AutoCloseableCloser() {}

    public static AutoCloseableCloser create()
    {
        return new AutoCloseableCloser();
    }

    public <C extends AutoCloseable> C register(C closeable)
    {
        requireNonNull(closeable, "closeable is null");
        stack.addFirst(closeable);
        return closeable;
    }

    @Override
    public void close()
            throws Exception
    {
        Throwable rootCause = null;
        while (!stack.isEmpty()) {
            AutoCloseable closeable = stack.removeFirst();
            try {
                closeable.close();
            }
            catch (Throwable t) {
                if (rootCause == null) {
                    rootCause = t;
                }
                else if (rootCause != t) {
                    // Self-suppression not permitted
                    rootCause.addSuppressed(t);
                }
            }
        }
        if (rootCause != null) {
            propagateIfPossible(rootCause, Exception.class);
            // not possible
            throw new AssertionError(rootCause);
        }
    }
}
