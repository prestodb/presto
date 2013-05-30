package com.facebook.presto.operator.scalar;

import com.google.common.base.Ascii;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public final class StringFunctions
{
    private StringFunctions() {}

    @ScalarFunction
    public static Slice chr(long n)
    {
        Slice slice = Slices.allocate(1);
        slice.setByte(0, Ints.saturatedCast(n));
        return slice;
    }

    @ScalarFunction
    public static Slice concat(Slice str1, Slice str2)
    {
        Slice concat = Slices.allocate(str1.length() + str2.length());
        concat.setBytes(0, str1);
        concat.setBytes(str1.length(), str2);
        return concat;
    }

    @ScalarFunction
    public static long length(Slice slice)
    {
        return slice.length();
    }

    @ScalarFunction
    public static Slice replace(Slice str, Slice search)
    {
        return replace(str, search, Slices.EMPTY_SLICE);
    }

    @ScalarFunction
    public static Slice replace(Slice str, Slice search, Slice replace)
    {
        String replaced = str.toString(Charsets.UTF_8).replace(
                search.toString(Charsets.UTF_8),
                replace.toString(Charsets.UTF_8));
        return Slices.copiedBuffer(replaced, Charsets.UTF_8);
    }

    @ScalarFunction
    public static Slice reverse(Slice slice)
    {
        Slice reverse = Slices.allocate(slice.length());
        for (int i = 0, j = slice.length() - 1; i < slice.length(); i++, j--) {
            reverse.setByte(j, slice.getByte(i));
        }
        return reverse;
    }

    @ScalarFunction("strpos")
    public static long stringPosition(Slice string, Slice substring)
    {
        if (substring.length() > string.length()) {
            return 0;
        }

        for (int i = 0; i <= (string.length() - substring.length()); i++) {
            if (string.equals(i, substring.length(), substring, 0, substring.length())) {
                return i + 1;
            }
        }

        return 0;
    }

    @ScalarFunction
    public static Slice substr(Slice slice, long start)
    {
        return substr(slice, start, slice.length());
    }

    @ScalarFunction
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

    @ScalarFunction("ltrim")
    public static Slice leftTrim(Slice slice)
    {
        int start = firstNonSpace(slice);
        return slice.slice(start, slice.length() - start);
    }

    @ScalarFunction("rtrim")
    public static Slice rightTrim(Slice slice)
    {
        int end = lastNonSpace(slice);
        return slice.slice(0, end + 1);
    }

    @ScalarFunction
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

    @ScalarFunction
    public static Slice lower(Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toLowerCase((char) slice.getByte(i)));
        }
        return upper;
    }

    @ScalarFunction
    public static Slice upper(Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toUpperCase((char) slice.getByte(i)));
        }
        return upper;
    }
}
