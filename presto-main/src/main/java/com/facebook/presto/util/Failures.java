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

import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Failure;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingException;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Functions.toStringFunction;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class Failures
{
    private static final String NODE_CRASHED_ERROR = "The node may have crashed or be under too much load. " +
            "This is probably a transient issue, so please retry your query in a few minutes.";

    public static final String WORKER_NODE_ERROR = "Encountered too many errors talking to a worker node. " + NODE_CRASHED_ERROR;

    public static final String WORKER_RESTARTED_ERROR = "A worker node running your query has restarted. " + NODE_CRASHED_ERROR;

    private Failures() {}

    public static ExecutionFailureInfo toFailure(Throwable failure)
    {
        if (failure == null) {
            return null;
        }
        // todo prevent looping with suppressed cause loops and such
        String type;
        if (failure instanceof Failure) {
            type = ((Failure) failure).getType();
        }
        else {
            type = failure.getClass().getCanonicalName();
        }

        ErrorCode errorCode = null;
        if (failure instanceof PrestoException) {
            errorCode = ((PrestoException) failure).getErrorCode();
        }
        else if (failure instanceof Failure) {
            errorCode = ((Failure) failure).getErrorCode();
        }

        return new ExecutionFailureInfo(type,
                failure.getMessage(),
                toFailure(failure.getCause()),
                toFailures(asList(failure.getSuppressed())),
                Lists.transform(asList(failure.getStackTrace()), toStringFunction()),
                getErrorLocation(failure),
                errorCode);
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }

    public static List<ExecutionFailureInfo> toFailures(Collection<? extends Throwable> failures)
    {
        return failures.stream()
                .map(Failures::toFailure)
                .collect(toImmutableList());
    }

    @Nullable
    private static ErrorLocation getErrorLocation(Throwable throwable)
    {
        // TODO: this is a big hack
        if (throwable instanceof ParsingException) {
            ParsingException e = (ParsingException) throwable;
            return new ErrorLocation(e.getLineNumber(), e.getColumnNumber());
        }
        return null;
    }
}
