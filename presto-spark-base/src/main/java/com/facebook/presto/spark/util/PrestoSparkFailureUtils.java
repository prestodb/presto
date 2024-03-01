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
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.spark.classloader_interface.ExecutionStrategy;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFailure;
import com.facebook.presto.spi.ErrorCause;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.execution.ExecutionFailureInfo.toStackTraceElement;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getRetryOnOutOfMemoryWithIncreasedMemoryErrorCodes;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryBroadcastJoinEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isRetryOnOutOfMemoryWithIncreasedMemoryEnabled;
import static com.facebook.presto.spark.classloader_interface.ExecutionStrategy.DISABLE_BROADCAST_JOIN;
import static com.facebook.presto.spark.classloader_interface.ExecutionStrategy.INCREASE_CONTAINER_SIZE;
import static com.facebook.presto.spark.classloader_interface.ExecutionStrategy.INCREASE_HASH_PARTITION_COUNT;
import static com.facebook.presto.spi.ErrorCause.LOW_PARTITION_COUNT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT;
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

        List<ExecutionStrategy> retryExecutionStrategies = getRetryExecutionStrategies(session,
                executionFailureInfo.getErrorCode(),
                executionFailureInfo.getMessage(),
                executionFailureInfo.getErrorCause());
        return new PrestoSparkFailure(
                prestoSparkFailure.getMessage(),
                prestoSparkFailure.getCause(),
                prestoSparkFailure.getType(),
                prestoSparkFailure.getErrorCode(),
                retryExecutionStrategies);
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
                ImmutableList.of());

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

    /**
     * Returns a list of retry strategies based on the provided error.
     */
    private static List<ExecutionStrategy> getRetryExecutionStrategies(Session session, ErrorCode errorCode, String message, ErrorCause errorCause)
    {
        if (errorCode == null || message == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<ExecutionStrategy> strategies = new ImmutableList.Builder<>();

        if (isRetryOnOutOfMemoryBroadcastJoinEnabled(session) &&
                errorCode.equals(EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT.toErrorCode())) {
            strategies.add(DISABLE_BROADCAST_JOIN);
        }

        if (isRetryOnOutOfMemoryWithIncreasedMemoryEnabled(session) &&
                getRetryOnOutOfMemoryWithIncreasedMemoryErrorCodes(session).contains(errorCode.getName().toUpperCase())) {
            strategies.add(INCREASE_CONTAINER_SIZE);
        }

        if (isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(session) &&
                LOW_PARTITION_COUNT == errorCause) {
            strategies.add(INCREASE_HASH_PARTITION_COUNT);
        }

        return strategies.build();
    }
}
