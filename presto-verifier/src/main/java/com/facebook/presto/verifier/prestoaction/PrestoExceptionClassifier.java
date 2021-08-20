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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.connector.thrift.ThriftErrorCode;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.spark.SparkErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.verifier.framework.ClusterConnectionException;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryStage;
import com.facebook.presto.verifier.framework.ThrottlingException;
import com.google.common.collect.ImmutableSet;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.facebook.presto.connector.thrift.ThriftErrorCode.THRIFT_SERVICE_CONNECTION_ERROR;
import static com.facebook.presto.connector.thrift.ThriftErrorCode.THRIFT_SERVICE_GENERIC_REMOTE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_OFFLINE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TABLE_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_TASK;
import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static com.facebook.presto.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_SETUP;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_SETUP;
import static com.google.common.base.Functions.identity;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class PrestoExceptionClassifier
        implements SqlExceptionClassifier
{
    private static final Pattern TABLE_ALREADY_EXISTS_PATTERN = Pattern.compile("table.*already exists", CASE_INSENSITIVE);

    private final Map<Integer, ErrorCodeSupplier> errorByCode;
    private final Set<ErrorCodeSupplier> retryableErrors;
    private final Set<ErrorMatcher> conditionalRetryableErrors;
    private final Set<ErrorCodeSupplier> resubmittedErrors;
    private final Set<ErrorMatcher> conditionalResubmittedErrors;

    private PrestoExceptionClassifier(
            Set<ErrorCodeSupplier> recognizedErrors,
            Set<ErrorCodeSupplier> retryableErrors,
            Set<ErrorMatcher> conditionalRetryableErrors,
            Set<ErrorCodeSupplier> resubmittedErrors,
            Set<ErrorMatcher> conditionalResubmittedErrors)
    {
        this.errorByCode = recognizedErrors.stream()
                .collect(toImmutableMap(errorCode -> errorCode.toErrorCode().getCode(), identity()));
        this.retryableErrors = ImmutableSet.copyOf(retryableErrors);
        this.conditionalRetryableErrors = ImmutableSet.copyOf(conditionalRetryableErrors);
        this.resubmittedErrors = ImmutableSet.copyOf(resubmittedErrors);
        this.conditionalResubmittedErrors = ImmutableSet.copyOf(conditionalResubmittedErrors);
    }

    public static Builder defaultBuilder()
    {
        return new Builder()
                .addRecognizedErrors(asList(StandardErrorCode.values()))
                .addRecognizedErrors(asList(HiveErrorCode.values()))
                .addRecognizedErrors(asList(JdbcErrorCode.values()))
                .addRecognizedErrors(asList(ThriftErrorCode.values()))
                .addRecognizedErrors(asList(SparkErrorCode.values()))
                // From StandardErrorCode
                .addRetryableError(NO_NODES_AVAILABLE)
                .addRetryableError(REMOTE_TASK_ERROR)
                .addRetryableError(REMOTE_TASK_MISMATCH)
                .addRetryableError(SERVER_SHUTTING_DOWN)
                .addRetryableError(SERVER_STARTING_UP)
                .addRetryableError(TOO_MANY_REQUESTS_FAILED)
                .addRetryableError(PAGE_TRANSPORT_ERROR)
                .addRetryableError(PAGE_TRANSPORT_TIMEOUT)
                .addRetryableError(REMOTE_HOST_GONE)
                .addRetryableError(ABANDONED_TASK)
                .addRetryableError(ABANDONED_QUERY)
                // From HiveErrorCode
                .addRetryableError(HIVE_CURSOR_ERROR)
                .addRetryableError(HIVE_FILE_NOT_FOUND)
                .addRetryableError(HIVE_TOO_MANY_OPEN_PARTITIONS)
                .addRetryableError(HIVE_WRITER_OPEN_ERROR)
                .addRetryableError(HIVE_WRITER_CLOSE_ERROR)
                .addRetryableError(HIVE_WRITER_DATA_ERROR)
                .addRetryableError(HIVE_FILESYSTEM_ERROR)
                .addRetryableError(HIVE_CANNOT_OPEN_SPLIT)
                .addRetryableError(HIVE_METASTORE_ERROR)
                // From JdbcErrorCode
                .addRetryableError(JDBC_ERROR)
                // From ThriftErrorCode
                .addRetryableError(THRIFT_SERVICE_CONNECTION_ERROR)
                .addRetryableError(THRIFT_SERVICE_GENERIC_REMOTE_ERROR)
                // Conditional Retryable Errors
                .addRetryableError(EXCEEDED_TIME_LIMIT, Optional.of(DESCRIBE), Optional.empty())
                // Resubmitted Errors
                .addResubmittedError(HIVE_PARTITION_DROPPED_DURING_QUERY)
                .addResubmittedError(HIVE_TABLE_DROPPED_DURING_QUERY)
                .addResubmittedError(CLUSTER_OUT_OF_MEMORY)
                .addResubmittedError(ADMINISTRATIVELY_PREEMPTED)
                // Conditional Resubmitted Errors
                .addResubmittedError(SYNTAX_ERROR, Optional.of(CONTROL_SETUP), Optional.of(TABLE_ALREADY_EXISTS_PATTERN))
                .addResubmittedError(SYNTAX_ERROR, Optional.of(TEST_SETUP), Optional.of(TABLE_ALREADY_EXISTS_PATTERN))
                .addResubmittedError(HIVE_PARTITION_OFFLINE, Optional.of(TEST_MAIN), Optional.empty());
    }

    public QueryException createException(QueryStage queryStage, QueryActionStats queryActionStats, SQLException cause)
    {
        Optional<Throwable> clusterConnectionExceptionCause = getClusterConnectionExceptionCause(cause);
        if (clusterConnectionExceptionCause.isPresent()) {
            return new ClusterConnectionException(clusterConnectionExceptionCause.get(), queryStage);
        }

        Optional<Throwable> requestThrottledExceptionCause = getRequestThrottleExceptionCause(cause);
        if (requestThrottledExceptionCause.isPresent()) {
            return new ThrottlingException(cause, queryStage);
        }

        Optional<ErrorCodeSupplier> errorCode = getErrorCode(cause.getErrorCode());
        boolean retryable = errorCode.isPresent() && isRetryable(errorCode.get(), queryStage, cause.getMessage());
        return new PrestoQueryException(cause, retryable, queryStage, errorCode, queryActionStats);
    }

    @Override
    public Optional<ErrorCodeSupplier> getErrorCode(int code)
    {
        return Optional.ofNullable(errorByCode.get(code));
    }

    @Override
    public boolean isRetryable(ErrorCodeSupplier errorCode, QueryStage queryStage, String message)
    {
        return retryableErrors.contains(errorCode)
                || conditionalRetryableErrors.stream().anyMatch(matcher -> matcher.matches(errorCode, queryStage, message));
    }

    public boolean shouldResubmit(Throwable throwable)
    {
        if (!(throwable instanceof PrestoQueryException)) {
            return false;
        }
        PrestoQueryException queryException = (PrestoQueryException) throwable;
        Optional<ErrorCodeSupplier> errorCode = queryException.getErrorCode();
        return errorCode.isPresent()
                && (resubmittedErrors.contains(errorCode.get())
                || conditionalResubmittedErrors.stream().anyMatch(matcher -> matcher.matches(errorCode.get(), queryException.getQueryStage(), queryException.getMessage())));
    }

    public static boolean isClusterConnectionException(Throwable t)
    {
        return getClusterConnectionExceptionCause(t).isPresent();
    }

    private static Optional<Throwable> getClusterConnectionExceptionCause(Throwable t)
    {
        while (t != null) {
            if (t instanceof SocketTimeoutException ||
                    t instanceof SocketException ||
                    t instanceof EOFException ||
                    t instanceof UncheckedIOException ||
                    t instanceof TimeoutException ||
                    (t.getClass().equals(RuntimeException.class) && t.getMessage() != null && t.getMessage().contains("Error fetching next at"))) {
                return Optional.of(t);
            }
            t = t.getCause();
        }
        return Optional.empty();
    }

    private static Optional<Throwable> getRequestThrottleExceptionCause(Throwable t)
    {
        while (t != null) {
            if (t instanceof RuntimeException && t.getMessage() != null && t.getMessage().contains("Request throttled")) {
                return Optional.of(t);
            }
            t = t.getCause();
        }
        return Optional.empty();
    }

    public static class Builder
    {
        private final ImmutableSet.Builder<ErrorCodeSupplier> recognizedErrors = ImmutableSet.builder();
        private final ImmutableSet.Builder<ErrorCodeSupplier> retryableErrors = ImmutableSet.builder();
        private final ImmutableSet.Builder<ErrorMatcher> conditionalRetryableErrors = ImmutableSet.builder();
        private final ImmutableSet.Builder<ErrorCodeSupplier> resubmittedErrors = ImmutableSet.builder();
        private final ImmutableSet.Builder<ErrorMatcher> conditionalResubmittedErrors = ImmutableSet.builder();

        private Builder()
        {
        }

        public Builder addRecognizedErrors(Iterable<ErrorCodeSupplier> errors)
        {
            this.recognizedErrors.addAll(errors);
            return this;
        }

        public Builder addRetryableError(ErrorCodeSupplier error)
        {
            this.retryableErrors.add(error);
            return this;
        }

        public Builder addRetryableError(ErrorCodeSupplier errorCode, Optional<QueryStage> queryStage, Optional<Pattern> errorMessagePattern)
        {
            this.conditionalRetryableErrors.add(new ErrorMatcher(errorCode, queryStage, errorMessagePattern));
            return this;
        }

        public Builder addResubmittedError(ErrorCodeSupplier error)
        {
            this.resubmittedErrors.add(error);
            return this;
        }

        public Builder addResubmittedError(ErrorCodeSupplier errorCode, Optional<QueryStage> queryStage, Optional<Pattern> errorMessagePattern)
        {
            this.conditionalResubmittedErrors.add(new ErrorMatcher(errorCode, queryStage, errorMessagePattern));
            return this;
        }

        public PrestoExceptionClassifier build()
        {
            Set<ErrorCodeSupplier> recognizedErrors = this.recognizedErrors.build();
            Set<ErrorCodeSupplier> retryableErrors = this.retryableErrors.build();
            Set<ErrorMatcher> conditionalRetryableErrors = this.conditionalRetryableErrors.build();
            Set<ErrorCodeSupplier> resubmittedErrors = this.resubmittedErrors.build();
            Set<ErrorMatcher> conditionalResubmittedErrors = this.conditionalResubmittedErrors.build();

            retryableErrors.forEach(error -> checkArgument(recognizedErrors.contains(error), "Error not recognized: %s", error));
            conditionalRetryableErrors.forEach(
                    errorMatcher -> checkArgument(recognizedErrors.contains(errorMatcher.getErrorCode()), "Error not recognized: %s", errorMatcher.getErrorCode()));
            resubmittedErrors.forEach(error -> checkArgument(recognizedErrors.contains(error), "Error not recognized: %s", error));
            conditionalResubmittedErrors.forEach(
                    errorMatcher -> checkArgument(recognizedErrors.contains(errorMatcher.getErrorCode()), "Error not recognized: %s", errorMatcher.getErrorCode()));

            return new PrestoExceptionClassifier(
                    recognizedErrors,
                    retryableErrors,
                    conditionalRetryableErrors,
                    resubmittedErrors,
                    conditionalResubmittedErrors);
        }
    }

    private static class ErrorMatcher
    {
        private final ErrorCodeSupplier errorCode;
        private final Optional<QueryStage> queryStage;
        private final Optional<Pattern> errorMessagePattern;

        public ErrorMatcher(
                ErrorCodeSupplier errorCode,
                Optional<QueryStage> queryStage,
                Optional<Pattern> errorMessagePattern)
        {
            this.errorCode = requireNonNull(errorCode, "errorCode is null");
            this.queryStage = requireNonNull(queryStage, "queryStage is null");
            this.errorMessagePattern = requireNonNull(errorMessagePattern, "errorMessagePattern is null");
        }

        public ErrorCodeSupplier getErrorCode()
        {
            return errorCode;
        }

        public boolean matches(ErrorCodeSupplier errorCode, QueryStage queryStage, String errorMessage)
        {
            return this.errorCode.equals(errorCode)
                    && (!this.queryStage.isPresent() || this.queryStage.get().equals(queryStage))
                    && (!this.errorMessagePattern.isPresent() || this.errorMessagePattern.get().matcher(errorMessage).find());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ErrorMatcher o = (ErrorMatcher) obj;
            return Objects.equals(errorCode, o.errorCode) &&
                    Objects.equals(queryStage, o.queryStage) &&
                    Objects.equals(errorMessagePattern, o.errorMessagePattern);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(errorCode, queryStage, errorMessagePattern);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("errorCode", errorCode)
                    .add("queryStage", queryStage)
                    .add("errorMessagePattern", errorMessagePattern)
                    .toString();
        }
    }
}
