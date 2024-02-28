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
package com.facebook.presto.util;

import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.util.Failures.toFailure;
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

    @Test
    public void testToLocalisedPrestoException()
    {
        Throwable syntaxError = new PrestoException("SYNTAX_ERROR_TOO_MANY_DOTS_IN_TABLE_NAME", SYNTAX_ERROR, "fake_table");
        Throwable localisedError = Failures.toLocalisedPrestoException(syntaxError, Locale.US);

        assertEquals(localisedError.getClass(), PrestoException.class);

        PrestoException localisedPrestoError = (PrestoException) localisedError;
        assertEquals(localisedPrestoError.getErrorCodeSupplier(), SYNTAX_ERROR);
        assertEquals(localisedPrestoError.getErrorCode(), SYNTAX_ERROR.toErrorCode());
        assertEquals(localisedPrestoError.getMessage(), "Too many dots in table name: fake_table");
        assertEquals(localisedPrestoError.getCause(), syntaxError);
    }

    @Test
    public void testToFailureWithLocalisedPrestoException()
    {
        Throwable syntaxError = new PrestoException("SYNTAX_ERROR_TOO_MANY_DOTS_IN_TABLE_NAME", SYNTAX_ERROR, "fake_table");
        Throwable localisedError = Failures.toLocalisedPrestoException(syntaxError, Locale.US);

        ExecutionFailureInfo failureInfo = toFailure(localisedError);
        assertEquals(failureInfo.getType(), "com.facebook.presto.spi.PrestoException");
        assertEquals(failureInfo.getMessage(), "Too many dots in table name: fake_table");
        assertEquals(failureInfo.getCause().getType(), "com.facebook.presto.spi.PrestoException");
        assertEquals(failureInfo.getCause().getMessage(), "SYNTAX_ERROR");
    }

    /**
     * Test toFailure method with a PrestoException that was intended to be localized
     * but somehow missed the localization call.
     */
    @Test
    public void testToFailureCatchAllScenario()
    {
        Throwable syntaxError = new PrestoException("SYNTAX_ERROR_TOO_MANY_DOTS_IN_TABLE_NAME", SYNTAX_ERROR, "fake_table");

        ExecutionFailureInfo failureInfo = toFailure(syntaxError);
        assertEquals(failureInfo.getType(), "com.facebook.presto.spi.PrestoException");
        assertEquals(failureInfo.getMessage(), "Too many dots in table name: fake_table");
        assertEquals(failureInfo.getCause().getType(), "com.facebook.presto.spi.PrestoException");
        assertEquals(failureInfo.getCause().getMessage(), "SYNTAX_ERROR");
    }
}
