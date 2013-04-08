/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Immutable
public class FailureInfo
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private final String type;
    private final String message;
    private final FailureInfo cause;
    private final List<FailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorLocation errorLocation;

    @JsonCreator
    public FailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") FailureInfo cause,
            @JsonProperty("suppressed") List<FailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorLocation") @Nullable ErrorLocation errorLocation)
    {
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(suppressed, "suppressed is null");
        Preconditions.checkNotNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorLocation = errorLocation;
    }

    @NotNull
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
    public FailureInfo getCause()
    {
        return cause;
    }

    @NotNull
    @JsonProperty
    public List<FailureInfo> getSuppressed()
    {
        return suppressed;
    }

    @NotNull
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

    public RuntimeException toException()
    {
        return toException(this);
    }

    private static Failure toException(FailureInfo failureInfo)
    {
        if (failureInfo == null) {
            return null;
        }
        Failure failure = new Failure(failureInfo.getType(), failureInfo.getMessage(), toException(failureInfo.getCause()));
        for (FailureInfo suppressed : failureInfo.getSuppressed()) {
            failure.addSuppressed(toException(suppressed));
        }
        ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
        for (String stack : failureInfo.getStack()) {
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

    private static class Failure
            extends RuntimeException
    {
        private final String type;

        private Failure(String type, String message, Failure cause)
        {
            super(message, cause, true, true);
            this.type = type;
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
