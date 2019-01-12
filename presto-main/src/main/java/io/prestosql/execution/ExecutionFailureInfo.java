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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.client.ErrorLocation;
import io.prestosql.client.FailureInfo;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.HostAddress;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class ExecutionFailureInfo
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private final String type;
    private final String message;
    private final ExecutionFailureInfo cause;
    private final List<ExecutionFailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorLocation errorLocation;
    private final ErrorCode errorCode;
    // use for transport errors
    private final HostAddress remoteHost;

    @JsonCreator
    public ExecutionFailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") ExecutionFailureInfo cause,
            @JsonProperty("suppressed") List<ExecutionFailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorLocation") @Nullable ErrorLocation errorLocation,
            @JsonProperty("errorCode") @Nullable ErrorCode errorCode,
            @JsonProperty("remoteHost") @Nullable HostAddress remoteHost)
    {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorLocation = errorLocation;
        this.errorCode = errorCode;
        this.remoteHost = remoteHost;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @Nullable
    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @JsonProperty
    public ExecutionFailureInfo getCause()
    {
        return cause;
    }

    @JsonProperty
    public List<ExecutionFailureInfo> getSuppressed()
    {
        return suppressed;
    }

    @JsonProperty
    public List<String> getStack()
    {
        return stack;
    }

    @Nullable
    @JsonProperty
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Nullable
    @JsonProperty
    public HostAddress getRemoteHost()
    {
        return remoteHost;
    }

    public FailureInfo toFailureInfo()
    {
        List<FailureInfo> suppressed = this.suppressed.stream()
                .map(ExecutionFailureInfo::toFailureInfo)
                .collect(toImmutableList());

        return new FailureInfo(type, message, cause == null ? null : cause.toFailureInfo(), suppressed, stack, errorLocation);
    }

    public RuntimeException toException()
    {
        return toException(this);
    }

    private static Failure toException(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo == null) {
            return null;
        }
        Failure failure = new Failure(executionFailureInfo.getType(), executionFailureInfo.getMessage(), executionFailureInfo.getErrorCode(), toException(executionFailureInfo.getCause()));
        for (ExecutionFailureInfo suppressed : executionFailureInfo.getSuppressed()) {
            failure.addSuppressed(toException(suppressed));
        }
        ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
        for (String stack : executionFailureInfo.getStack()) {
            stackTraceBuilder.add(toStackTraceElement(stack));
        }
        ImmutableList<StackTraceElement> stackTrace = stackTraceBuilder.build();
        failure.setStackTrace(stackTrace.toArray(new StackTraceElement[stackTrace.size()]));
        return failure;
    }

    public static StackTraceElement toStackTraceElement(String stack)
    {
        Matcher matcher = STACK_TRACE_PATTERN.matcher(stack);
        if (matcher.matches()) {
            String declaringClass = matcher.group(1);
            String methodName = matcher.group(2);
            String fileName = matcher.group(3);
            int number = -1;
            if (fileName.equals("Native Method")) {
                fileName = null;
                number = -2;
            }
            else if (matcher.group(4) != null) {
                number = Integer.parseInt(matcher.group(4));
            }
            return new StackTraceElement(declaringClass, methodName, fileName, number);
        }
        return new StackTraceElement("Unknown", stack, null, -1);
    }
}
