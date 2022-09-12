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
package com.facebook.presto.spark.util;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFailure;
import com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy;
import com.facebook.presto.spi.ErrorCause;
import com.facebook.presto.spi.ErrorCode;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.execution.ExecutionFailureInfo.toStackTraceElement;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryBroadcastJoinEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryWithIncreasedMemoryEnabled;
import static com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy.DISABLE_BROADCAST_JOIN;
import static com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy.INCREASE_CONTAINER_SIZE;
import static com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy.INCREASE_HASH_PARTITION_COUNT;
import static com.facebook.presto.spi.ErrorCause.LOW_PARTITION_COUNT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrestoSparkFailureUtils
{
    private PrestoSparkFailureUtils() {}

    public static PrestoSparkFailure toPrestoSparkFailure(Session session, ExecutionFailureInfo executionFailureInfo)
    {
        requireNonNull(executionFailureInfo, "executionFailureInfo is null");
        PrestoSparkFailure prestoSparkFailure = toPrestoSparkFailure(executionFailureInfo);
        checkState(prestoSparkFailure != null);

        Optional<RetryExecutionStrategy> retryExecutionStrategy = getRetryExecutionStrategy(session,
                executionFailureInfo.getErrorCode(),
                executionFailureInfo.getMessage(),
                executionFailureInfo.getErrorCause());
        return new PrestoSparkFailure(
                prestoSparkFailure.getMessage(),
                prestoSparkFailure.getCause(),
                prestoSparkFailure.getType(),
                prestoSparkFailure.getErrorCode(),
                retryExecutionStrategy);
    }

    @Nullable
    private static PrestoSparkFailure toPrestoSparkFailure(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo == null) {
            return null;
        }

        PrestoSparkFailure prestoSparkFailure = new PrestoSparkFailure(
                executionFailureInfo.getMessage(),
                toPrestoSparkFailure(executionFailureInfo.getCause()),
                executionFailureInfo.getType(),
                executionFailureInfo.getErrorCode() == null ? "" : executionFailureInfo.getErrorCode().getName(),
                Optional.empty());

        for (ExecutionFailureInfo suppressed : executionFailureInfo.getSuppressed()) {
            prestoSparkFailure.addSuppressed(requireNonNull(toPrestoSparkFailure(suppressed), "suppressed failure is null"));
        }
        ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
        for (String stack : executionFailureInfo.getStack()) {
            stackTraceBuilder.add(toStackTraceElement(stack));
        }
        List<StackTraceElement> stackTrace = stackTraceBuilder.build();
        prestoSparkFailure.setStackTrace(stackTrace.toArray(new StackTraceElement[stackTrace.size()]));
        return prestoSparkFailure;
    }

    private static Optional<RetryExecutionStrategy> getRetryExecutionStrategy(Session session, ErrorCode errorCode, String message, ErrorCause errorCause)
    {
        if (errorCode == null || message == null) {
            return Optional.empty();
        }

        if (isRetryOnOutOfMemoryBroadcastJoinEnabled(session) &&
                errorCode.equals(EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT.toErrorCode())) {
            return Optional.of(DISABLE_BROADCAST_JOIN);
        }

        if (isRetryOnOutOfMemoryWithIncreasedMemoryEnabled(session) && errorCode.equals(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode())) {
            return Optional.of(INCREASE_CONTAINER_SIZE);
        }

        if (isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(session) &&
                LOW_PARTITION_COUNT == errorCause) {
            return Optional.of(INCREASE_HASH_PARTITION_COUNT);
        }

        return Optional.empty();
    }
}
