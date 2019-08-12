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
package com.facebook.presto.verifier.retry;

import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryStage;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.retry.RetryDriver.RetryOperation;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static io.airlift.log.Level.DEBUG;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestRetryDriver
{
    private static class MockOperation
            implements RetryOperation<Integer>
    {
        private final int succeedWithNumCalls;
        private final QueryException exception;
        private int callCount;

        public MockOperation(int succeedWithNumCalls, QueryException exception)
        {
            this.succeedWithNumCalls = succeedWithNumCalls;
            this.exception = requireNonNull(exception, "exception is null");
        }

        @Override
        public Integer run()
                throws QueryException
        {
            callCount++;
            if (callCount >= succeedWithNumCalls) {
                return callCount;
            }
            throw exception;
        }
    }

    static {
        Logging.initialize().setLevel(RetryDriver.class.getName(), DEBUG);
    }

    private static final QueryStage QUERY_STAGE = CONTROL_MAIN;
    private static final QueryException RETRYABLE_EXCEPTION = QueryException.forClusterConnection(new SocketTimeoutException(), QUERY_STAGE);
    private static final QueryException NON_RETRYABLE_EXCEPTION = QueryException.forPresto(new RuntimeException(), Optional.of(REMOTE_HOST_GONE), false, Optional.empty(), QUERY_STAGE);
    private static final RetryDriver RETRY_DRIVER = new RetryDriver(
            new RetryConfig()
                    .setMaxAttempts(5)
                    .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                    .setMaxBackoffDelay(new Duration(100, MILLISECONDS))
                    .setScaleFactor(2),
            QueryException::isRetryable);

    @Test
    public void testSuccess()
    {
        assertEquals(
                RETRY_DRIVER.run("test", new VerificationContext(), new MockOperation(5, RETRYABLE_EXCEPTION)),
                Integer.valueOf(5));
    }

    @Test(expectedExceptions = QueryException.class)
    public void testMaxAttemptsExceeded()
    {
        RETRY_DRIVER.run("test", new VerificationContext(), new MockOperation(6, RETRYABLE_EXCEPTION));
    }

    @Test(expectedExceptions = QueryException.class)
    public void testNonRetryableFailure()
    {
        RETRY_DRIVER.run("test", new VerificationContext(), new MockOperation(3, NON_RETRYABLE_EXCEPTION));
    }

    @Test(timeOut = 5000)
    public void testBackoffTimeCapped()
    {
        RetryDriver retryDriver = new RetryDriver(
                new RetryConfig()
                        .setMaxAttempts(5)
                        .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                        .setMaxBackoffDelay(new Duration(100, MILLISECONDS))
                        .setScaleFactor(1000),
                QueryException::isRetryable);
        retryDriver.run("test", new VerificationContext(), new MockOperation(5, RETRYABLE_EXCEPTION));
    }
}
