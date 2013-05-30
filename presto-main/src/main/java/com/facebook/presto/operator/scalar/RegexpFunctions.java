package com.facebook.presto.operator.scalar;

import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegexpFunctions
{
    private static final ThreadLocalCache<Slice, Pattern> CACHE = new ThreadLocalCache<Slice, Pattern>(100)
    {
        @Override
        protected Pattern load(Slice pattern)
        {
            return Pattern.compile(pattern.toString(UTF_8));
        }
    };

    private static Matcher matcher(Slice source, Slice pattern)
    {
        return CACHE.get(pattern).matcher(source.toString(UTF_8));
    }

    private RegexpFunctions() {}

    @ScalarFunction
    public static boolean regexpLike(Slice source, Slice pattern)
    {
        return matcher(source, pattern).find();
    }

    @ScalarFunction
    public static Slice regexpReplace(Slice source, Slice pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @ScalarFunction
    public static Slice regexpReplace(Slice source, Slice pattern, Slice replacement)
    {
        Matcher matcher = matcher(source, pattern);
        String replaced = matcher.replaceAll(replacement.toString(UTF_8));
        return Slices.copiedBuffer(replaced, UTF_8);
    }

    @ScalarFunction
    public static Slice regexpExtract(Slice source, Slice pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @ScalarFunction
    public static Slice regexpExtract(Slice source, Slice pattern, long group)
    {
        Matcher matcher = matcher(source, pattern);
        if ((group < 0) || (group > matcher.groupCount())) {
            throw new IllegalArgumentException("invalid group count");
        }
        if (!matcher.find()) {
            return null;
        }
        String extracted = matcher.group(Ints.checkedCast(group));
        return Slices.copiedBuffer(extracted, UTF_8);
    }
}
