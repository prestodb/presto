/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.parser.ParsingException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;

public class FailureInfo
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    public static FailureInfo toFailure(Throwable failure)
    {
        if (failure == null) {
            return null;
        }
        // todo prevent looping with suppressed cause loops and such
        return new FailureInfo(failure.getClass().getCanonicalName(),
                failure.getMessage(),
                toFailure(failure.getCause()),
                toFailures(asList(failure.getSuppressed())),
                Lists.transform(asList(failure.getStackTrace()), toStringFunction()),
                getErrorLocation(failure));
    }

    public static List<FailureInfo> toFailures(Iterable<? extends Throwable> failures)
    {
        return ImmutableList.copyOf(transform(failures, toFailureFunction()));
    }

    private static Function<Throwable, FailureInfo> toFailureFunction()
    {
        return new Function<Throwable, FailureInfo>()
        {
            @Override
            public FailureInfo apply(Throwable throwable)
            {
                return toFailure(throwable);
            }
        };
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

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @JsonProperty
    public FailureInfo getCause()
    {
        return cause;
    }

    @JsonProperty
    public List<FailureInfo> getSuppressed()
    {
        return suppressed;
    }

    @JsonProperty
    public List<String> getStack()
    {
        return stack;
    }

    @JsonProperty
    @Nullable
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

    public static Function<FailureInfo, String> messageGetter()
    {
        return new Function<FailureInfo, String>()
        {
            @Override
            public String apply(FailureInfo input)
            {
                return input.getMessage();
            }
        };
    }
}
