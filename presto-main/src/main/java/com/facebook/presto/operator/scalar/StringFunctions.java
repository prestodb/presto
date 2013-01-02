package com.facebook.presto.operator.scalar;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Ascii;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.FunctionRegistry.lookupStatic;

@SuppressWarnings("UnusedDeclaration")
public final class StringFunctions
{
    public static final MethodHandle CONCAT = lookupStatic(StringFunctions.class, "concat", Slice.class, Slice.class, Slice.class);
    public static final MethodHandle LENGTH = lookupStatic(StringFunctions.class, "length", long.class, Slice.class);
    public static final MethodHandle REVERSE = lookupStatic(StringFunctions.class, "reverse", Slice.class, Slice.class);
    public static final MethodHandle SUBSTR = lookupStatic(StringFunctions.class, "substr", Slice.class, Slice.class, long.class, long.class);
    public static final MethodHandle LTRIM = lookupStatic(StringFunctions.class, "leftTrim", Slice.class, Slice.class);
    public static final MethodHandle RTRIM = lookupStatic(StringFunctions.class, "rightTrim", Slice.class, Slice.class);
    public static final MethodHandle TRIM = lookupStatic(StringFunctions.class, "trim", Slice.class, Slice.class);
    public static final MethodHandle LOWER = lookupStatic(StringFunctions.class, "lower", Slice.class, Slice.class);
    public static final MethodHandle UPPER = lookupStatic(StringFunctions.class, "upper", Slice.class, Slice.class);

    private StringFunctions() {}

    public static Slice concat(Slice str1, Slice str2)
    {
        Slice concat = Slices.allocate(str1.length() + str2.length());
        concat.setBytes(0, str1);
        concat.setBytes(str1.length(), str2);
        return concat;
    }

    public static long length(Slice slice)
    {
        return slice.length();
    }

    public static Slice reverse(Slice slice)
    {
        Slice reverse = Slices.allocate(slice.length());
        for (int i = 0, j = slice.length() - 1; i < slice.length(); i++, j--) {
            reverse.setByte(j, slice.getByte(i));
        }
        return reverse;
    }

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

    public static Slice leftTrim(Slice slice)
    {
        int start = firstNonSpace(slice);
        return slice.slice(start, slice.length() - start);
    }

    public static Slice rightTrim(Slice slice)
    {
        int end = lastNonSpace(slice);
        return slice.slice(0, end + 1);
    }

    public static Slice trim(Slice slice)
    {
        int start = firstNonSpace(slice);
        if (start == slice.length()) {
            return Slices.EMPTY_SLICE;
        }

        int end = lastNonSpace(slice);
        assert (end >= 0) && (end >= start);

        return slice.slice(start, (end - start) + 1);
    }

    private static int firstNonSpace(Slice slice)
    {
        for (int i = 0; i < slice.length(); i++) {
            if (slice.getByte(i) != ' ') {
                return i;
            }
        }
        return slice.length();
    }

    private static int lastNonSpace(Slice slice)
    {
        for (int i = slice.length() - 1; i >= 0; i--) {
            if (slice.getByte(i) != ' ') {
                return i;
            }
        }
        return -1;
    }

    public static Slice lower(Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toLowerCase((char) slice.getByte(i)));
        }
        return upper;
    }

    public static Slice upper(Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toUpperCase((char) slice.getByte(i)));
        }
        return upper;
    }
}
