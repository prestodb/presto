package com.facebook.presto.util;

import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.Failure;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.sql.parser.ParsingException;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;

public final class Failures
{
    private Failures() {}

    public static FailureInfo toFailure(Throwable failure)
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

        return new FailureInfo(type,
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
}
