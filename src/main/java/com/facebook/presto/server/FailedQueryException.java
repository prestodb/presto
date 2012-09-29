/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.common.collect.ImmutableList;

public class FailedQueryException extends RuntimeException
{
    public FailedQueryException(Throwable... causes)
    {
        this(ImmutableList.copyOf(causes));
    }

    public FailedQueryException(Iterable<Throwable> causes)
    {
        for (Throwable cause : causes) {
            addSuppressed(cause);
        }
    }
}
