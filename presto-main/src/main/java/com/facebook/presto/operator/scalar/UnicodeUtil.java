package com.facebook.presto.operator.scalar;


import io.airlift.slice.Slice;

final class UnicodeUtil {

    private UnicodeUtil() {
    }

    /**
     * Counts the code points within UTF8 encoded slice.
     */
    static int countCodePoints(final Slice slice) {
        return slice.toStringUtf8().length();
    }
}
