package com.facebook.presto.util;

import com.google.common.base.Optional;

public final class Optionals
{
    private Optionals() {}

    @Deprecated
    public static <T> Optional<T> guavaOptional(java.util.Optional<T> optional)
    {
        return Optional.fromNullable(optional.orElse(null));
    }

    @Deprecated
    public static <T> java.util.Optional<T> jdkOptional(Optional<T> optional)
    {
        return java.util.Optional.ofNullable(optional.orNull());
    }
}
