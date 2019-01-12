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

import org.testng.annotations.Test;

import static com.google.common.base.Throwables.propagateIfPossible;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAutoCloseableCloser
{
    @Test
    public void testEmpty()
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        closer.close();
    }

    @Test
    public void testAllClosed()
    {
        assertAllClosed(succeedingCloseable(), succeedingCloseable());
        assertAllClosed(failingCloseable(new RuntimeException()), failingCloseable(new RuntimeException()));
        assertAllClosed(failingCloseable(new Exception()), failingCloseable(new Exception()));
        assertAllClosed(failingCloseable(new Error()), failingCloseable(new Error()));
        assertAllClosed(failingCloseable(new Throwable()), failingCloseable(new Throwable()));
        assertAllClosed(failingCloseable(new Throwable()), failingCloseable(new Throwable()), failingCloseable(new Throwable()));
    }

    @Test
    public void testSuppressedException()
    {
        RuntimeException runtimeException = new RuntimeException();
        Exception exception = new Exception();
        Error error = new Error();

        AutoCloseableCloser closer = AutoCloseableCloser.create();
        // add twice to test self suppression handling
        closer.register(failingCloseable(error));
        closer.register(failingCloseable(error));
        closer.register(failingCloseable(exception));
        closer.register(failingCloseable(exception));
        closer.register(failingCloseable(runtimeException));
        closer.register(failingCloseable(runtimeException));

        try {
            closer.close();
            fail("expected to fail");
        }
        catch (Throwable t) {
            assertSame(t, runtimeException);
            assertSame(t.getSuppressed()[0], exception);
            assertSame(t.getSuppressed()[1], exception);
            assertSame(t.getSuppressed()[2], error);
            assertSame(t.getSuppressed()[3], error);
        }
    }

    private static void assertAllClosed(TestAutoCloseable... closeables)
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        for (AutoCloseable closeable : closeables) {
            closer.register(closeable);
        }
        try {
            closer.close();
        }
        catch (Throwable ignored) {
        }
        for (TestAutoCloseable closeable : closeables) {
            assertTrue(closeable.isClosed());
        }
    }

    private static TestAutoCloseable succeedingCloseable()
    {
        return new TestAutoCloseable(null);
    }

    private static TestAutoCloseable failingCloseable(Throwable t)
    {
        return new TestAutoCloseable(t);
    }

    private static class TestAutoCloseable
            implements AutoCloseable
    {
        private final Throwable failure;
        private boolean closed;

        private TestAutoCloseable(Throwable failure)
        {
            this.failure = failure;
        }

        public boolean isClosed()
        {
            return closed;
        }

        @Override
        public void close()
                throws Exception
        {
            closed = true;
            if (failure != null) {
                propagateIfPossible(failure, Exception.class);
                // not possible
                throw new AssertionError(failure);
            }
        }
    }
}
