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
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

import static java.util.Objects.requireNonNull;

/**
 * A retry policy that downgrades consistency level to LOCAL_ONE when retrying.
 * <p>
 * This policy is designed for scenarios where the application can tolerate
 * temporary degradation of consistency guarantees. Unlike the driver's built-in
 * ConsistencyDowngradingRetryPolicy which has complex downgrading logic, this
 * policy always downgrades to LOCAL_ONE for simplicity and predictability.
 * <p>
 * <b>Warning:</b> This may break consistency invariants! For example, if your
 * application relies on immediate write visibility by writing and reading at
 * QUORUM, downgrading to LOCAL_ONE could cause writes to go unnoticed by
 * subsequent reads. Use this policy only if you understand the consequences.
 * <p>
 * <b>Note:</b> In Driver 4.x, consistency level changes must be applied to the
 * statement before retrying. This policy returns RETRY_SAME or RETRY_NEXT, and
 * the application code must handle downgrading the consistency level on the
 * statement itself when needed.
 *
 * @see <a href="https://apache.github.io/cassandra-java-driver/4.19.0/core/retries/">Driver 4.x Retry Documentation</a>
 */
public class LocalOneDowngradingRetryPolicy
        implements RetryPolicy
{
    public static final LocalOneDowngradingRetryPolicy INSTANCE = new LocalOneDowngradingRetryPolicy(null, null);

    private static final int MAX_RETRIES = 1;

    public LocalOneDowngradingRetryPolicy(DriverContext context, String profileName)
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

        if (retryCount >= MAX_RETRIES) {
            return RetryDecision.RETHROW;
        }

        // If enough replicas responded to meet the consistency level,
        // but data was not present, retry at same consistency level
        if (received >= blockFor && !dataPresent) {
            return RetryDecision.RETRY_SAME;
        }

        // If not enough replicas responded, we would want to downgrade to LOCAL_ONE
        // In driver 4.x, we signal retry and the application must handle CL downgrade
        if (received < blockFor && cl != DefaultConsistencyLevel.LOCAL_ONE) {
            // Note: The actual CL downgrade must be done by modifying the statement
            // before the retry. This policy just signals that a retry should happen.
            return RetryDecision.RETRY_SAME;
        }

        // Already at LOCAL_ONE or can't downgrade further
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

        if (retryCount >= MAX_RETRIES) {
            return RetryDecision.RETHROW;
        }

        // Handle different write types
        if (writeType == DefaultWriteType.SIMPLE || writeType == DefaultWriteType.BATCH) {
            // For simple and batch writes, if at least one replica acknowledged,
            // ignore the exception (the write will eventually be replicated)
            if (received > 0) {
                return RetryDecision.IGNORE;
            }
            // No replica acknowledged, abort
            return RetryDecision.RETHROW;
        }
        else if (writeType == DefaultWriteType.UNLOGGED_BATCH) {
            // For unlogged batches, the write might have been persisted only partially.
            // Signal retry (application must downgrade CL to LOCAL_ONE if not already)
            if (cl != DefaultConsistencyLevel.LOCAL_ONE) {
                return RetryDecision.RETRY_SAME;
            }
            return RetryDecision.RETHROW;
        }
        else if (writeType == DefaultWriteType.BATCH_LOG) {
            // The peers chosen to receive the distributed batch log failed to respond.
            // Retry with same consistency level (coordinator will pick different peers)
            return RetryDecision.RETRY_SAME;
        }
        else {
            // Other write types should not be retried
            return RetryDecision.RETHROW;
        }
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

        if (retryCount >= MAX_RETRIES) {
            return RetryDecision.RETHROW;
        }

        // If at least one replica is alive, signal retry
        // Application must downgrade CL to LOCAL_ONE if not already at that level
        if (alive > 0 && cl != DefaultConsistencyLevel.LOCAL_ONE) {
            return RetryDecision.RETRY_SAME;
        }

        // No replicas alive or already at LOCAL_ONE - can't downgrade further
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onRequestAborted(
            Request request,
            Throwable error,
            int retryCount)
    {
        requireNonNull(request, "request is null");
        requireNonNull(error, "error is null");

        // Try next host for aborted requests (connection closed, heartbeat failure, etc.)
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

        // Try next host for coordinator errors (overloaded, server error, etc.)
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public void close()
    {
        // No resources to clean up
    }
}
