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
package com.facebook.presto.sidecar;

import com.facebook.presto.common.ErrorCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * This class provides failure information in the response from sidecar.
 * It is derived from {@link com.facebook.presto.client.FailureInfo}.
 */
@Immutable
public class NativeSidecarFailureInfo
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private final String type;
    private final String message;
    private final NativeSidecarFailureInfo cause;
    private final List<NativeSidecarFailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorCode errorCode;

    @JsonCreator
    public NativeSidecarFailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") NativeSidecarFailureInfo cause,
            @JsonProperty("suppressed") List<NativeSidecarFailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorCode") ErrorCode errorCode)
    {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorCode = errorCode;
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
    public NativeSidecarFailureInfo getCause()
    {
        return cause;
    }

    @JsonProperty
    public List<NativeSidecarFailureInfo> getSuppressed()
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
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public RuntimeException toException()
    {
        return toException(this);
    }

    private static FailureException toException(NativeSidecarFailureInfo failureInfo)
    {
        if (failureInfo == null) {
            return null;
        }
        FailureException failure = new FailureException(failureInfo.getType(), failureInfo.getMessage(), toException(failureInfo.getCause()));
        for (NativeSidecarFailureInfo suppressed : failureInfo.getSuppressed()) {
            failure.addSuppressed(toException(suppressed));
        }
        StackTraceElement[] stackTrace =
                failureInfo.getStack().stream().map(NativeSidecarFailureInfo::toStackTraceElement).toArray(StackTraceElement[]::new);
        failure.setStackTrace(stackTrace);
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

    private static class FailureException
            extends RuntimeException
    {
        private final String type;

        FailureException(String type, String message, FailureException cause)
        {
            super(message, cause);
            this.type = requireNonNull(type, "type is null");
        }

        public String getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            String message = getMessage();
            if (message != null) {
                return type + ": " + message;
            }
            return type;
        }
    }
}
