package com.facebook.presto.operator.scalar;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.FunctionRegistry.lookupStatic;

public class StringFunctions
{
    public static final MethodHandle SUBSTR = lookupStatic(StringFunctions.class, "substr", Slice.class, Slice.class, long.class, long.class);

    public static Slice substr(Slice slice, long start, long length)
    {
        if ((start == 0) || (length <= 0)) {
            return Slices.EMPTY_SLICE;
        }

        if (start > 0) {
            // make start zero-based
            start--;
        }
        else {
            // negative start is relative to end of string
            start += slice.length();
            if (start < 0) {
                return Slices.EMPTY_SLICE;
            }
        }

        if ((start + length) > slice.length()) {
            length = slice.length() - start;
        }
        if (start >= slice.length()) {
            return Slices.EMPTY_SLICE;
        }

        return slice.slice((int) start, (int) length);
    }
}
