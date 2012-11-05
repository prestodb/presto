package com.facebook.presto.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public class Ranges
{
    public static Range intersect(Iterable<Range> ranges)
    {
        Preconditions.checkNotNull(ranges, "ranges is null");
        Preconditions.checkArgument(!Iterables.isEmpty(ranges), "ranges is empty");

        Iterator<Range> iterator = ranges.iterator();

        Range result = iterator.next();
        while (iterator.hasNext()) {
            result = result.intersect(iterator.next());
        }

        return result;
    }

    public static Range merge(Iterable<Range> ranges)
    {
        Preconditions.checkNotNull(ranges, "ranges is null");
        Preconditions.checkArgument(!Iterables.isEmpty(ranges), "ranges is empty");

        Iterator<Range> iterator = ranges.iterator();

        Range result = iterator.next();
        while (iterator.hasNext()) {
            result = result.merge(iterator.next());
        }

        return result;
    }
}
