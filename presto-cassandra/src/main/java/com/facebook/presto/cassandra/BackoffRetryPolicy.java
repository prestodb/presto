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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

/**
 * Retry policy with exponential backoff for unavailable errors.
 * This policy implements backoff with jitter for unavailable errors,
 * while delegating other retry decisions to the default policy behavior.
 */
public class BackoffRetryPolicy
        implements RetryPolicy
{
    public static final BackoffRetryPolicy INSTANCE = new BackoffRetryPolicy(null, null);

    private static final int MAX_RETRIES = 10;
    private static final int BASE_DELAY_MS = 100;
    private static final int JITTER_MS = 100;

    public BackoffRetryPolicy(DriverContext context, String profileName)
    {
        // Constructor required by driver 4.x for policy instantiation
        // Context and profileName can be used for advanced configuration if needed
    }

    @Override
    public RetryDecision onReadTimeout(
            Request request,
            ConsistencyLevel cl,
            int blockFor,
            int received,
            boolean dataPresent,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(cl, "cl is null");
        // Delegate to default behavior: retry if data was present and we got enough responses
        if (dataPresent && received >= blockFor) {
            return RetryDecision.RETRY_SAME;
        }
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onWriteTimeout(
            Request request,
            ConsistencyLevel cl,
            WriteType writeType,
            int blockFor,
            int received,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(cl, "cl is null");
        requireNonNull(writeType, "writeType is null");
        // Delegate to default behavior: only retry for BATCH_LOG writes
        if (writeType == WriteType.BATCH_LOG) {
            return RetryDecision.RETRY_SAME;
        }
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onUnavailable(
            Request request,
            ConsistencyLevel cl,
            int required,
            int alive,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(cl, "cl is null");
        // Implement backoff with jitter for unavailable errors
        if (retryCount >= MAX_RETRIES) {
            return RetryDecision.RETHROW;
        }

        try {
            int jitter = ThreadLocalRandom.current().nextInt(JITTER_MS);
            int delay = (BASE_DELAY_MS * (retryCount + 1)) + jitter;
            Thread.sleep(delay);
            return RetryDecision.RETRY_SAME;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RetryDecision.RETHROW;
        }
    }

    @Override
    public RetryDecision onRequestAborted(
            Request request,
            Throwable error,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(error, "error is null");
        // Try next host for aborted requests
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public RetryDecision onErrorResponse(
            Request request,
            CoordinatorException error,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(error, "error is null");
        // Try next host for coordinator errors
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public void close()
    {
        // No resources to clean up
    }
}
