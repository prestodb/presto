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

import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static io.prestosql.util.Failures.toFailure;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestFailures
{
    @Test
    public void testToFailureLoop()
    {
        Throwable exception1 = new PrestoException(TOO_MANY_REQUESTS_FAILED, "fake exception 1");
        Throwable exception2 = new RuntimeException("fake exception 2", exception1);
        exception1.addSuppressed(exception2);

        // add exception 1 --> add suppress (exception 2) --> add cause (exception 1)
        ExecutionFailureInfo failure = toFailure(exception1);
        assertEquals(failure.getMessage(), "fake exception 1");
        assertNull(failure.getCause());
        assertEquals(failure.getSuppressed().size(), 1);
        assertEquals(failure.getSuppressed().get(0).getMessage(), "fake exception 2");
        assertEquals(failure.getErrorCode(), TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 2 --> add cause (exception 2) --> add suppress (exception 1)
        failure = toFailure(exception2);
        assertEquals(failure.getMessage(), "fake exception 2");
        assertNotNull(failure.getCause());
        assertEquals(failure.getCause().getMessage(), "fake exception 1");
        assertEquals(failure.getSuppressed().size(), 0);
        assertEquals(failure.getErrorCode(), TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 1 --> add suppress (exception 2) --> add suppress (exception 1)
        exception1 = new PrestoException(TOO_MANY_REQUESTS_FAILED, "fake exception 1");
        exception2 = new RuntimeException("fake exception 2");
        exception1.addSuppressed(exception2);
        exception2.addSuppressed(exception1);
        failure = toFailure(exception1);
        assertEquals(failure.getMessage(), "fake exception 1");
        assertNull(failure.getCause());
        assertEquals(failure.getSuppressed().size(), 1);
        assertEquals(failure.getSuppressed().get(0).getMessage(), "fake exception 2");
        assertEquals(failure.getErrorCode(), TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 2 --> add cause (exception 1) --> add cause (exception 2)
        exception1 = new RuntimeException("fake exception 1");
        exception2 = new RuntimeException("fake exception 2", exception1);
        exception1.initCause(exception2);
        failure = toFailure(exception2);
        assertEquals(failure.getMessage(), "fake exception 2");
        assertNotNull(failure.getCause());
        assertEquals(failure.getCause().getMessage(), "fake exception 1");
        assertEquals(failure.getSuppressed().size(), 0);
        assertEquals(failure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
    }
}
