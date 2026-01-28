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
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

import javax.annotation.Nonnull;

import java.util.concurrent.ThreadLocalRandom;

public class BackoffRetryPolicy
        implements RetryPolicy
{
    public static final BackoffRetryPolicy INSTANCE = new BackoffRetryPolicy();
    private static final int MAX_RETRIES = 10;

    private BackoffRetryPolicy() {}

    @Override
    public RetryDecision onReadTimeout(
            @Nonnull Request request,
            @Nonnull ConsistencyLevel cl,
            int blockFor,
            int received,
            boolean dataPresent,
            int retryCount)
    {
        // Delegate to default behavior for read timeouts
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onWriteTimeout(
            @Nonnull Request request,
            @Nonnull ConsistencyLevel cl,
            @Nonnull WriteType writeType,
            int blockFor,
            int received,
            int retryCount)
    {
        // Delegate to default behavior for write timeouts
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onUnavailable(
            @Nonnull Request request,
            @Nonnull ConsistencyLevel cl,
            int required,
            int alive,
            int retryCount)
    {
        if (retryCount >= MAX_RETRIES) {
            return RetryDecision.RETHROW;
        }

        try {
            // Exponential backoff with jitter
            int jitter = ThreadLocalRandom.current().nextInt(100);
            int delay = (100 * (retryCount + 1)) + jitter;
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
            @Nonnull Request request,
            @Nonnull Throwable error,
            int retryCount)
    {
        // Try next host on request aborted
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public RetryDecision onErrorResponse(
            @Nonnull Request request,
            @Nonnull CoordinatorException error,
            int retryCount)
    {
        // Try next host on coordinator errors
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public void close()
    {
        // No resources to clean up
    }
}
