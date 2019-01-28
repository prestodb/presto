/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.spi.SubfieldPath;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.facebook.presto.spi.block.ByteArrayUtils.memcmp;;

public class Filters
{
    public static class IsNull
        extends Filter
    {
        @Override
        public boolean testNull()
        {
                return true;
            }
    }

    public static class BigintRange
            extends Filter
    {
        private final long lower;
        private final long upper;

        BigintRange(long lower, long upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean testLong(long value)
        {
            return value >= lower && value <= upper;
        }

        
        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (upper == lower) {
                return 1;
            }
            return upper != Long.MAX_VALUE && lower != Long.MIN_VALUE ? 2 : 3;
        }

        public long getLower()
        {
            return lower;
        }

                public long getUpper()
        {
            return upper;
        }

        @Override
        public boolean isEquality()
        {
            return upper == lower;
        }
    }

    public static class DoubleRange
            extends Filter
    {
        private final double lower;
        private final boolean lowerUnbounded;
        private final boolean lowerExclusive;
        private final double upper;
        private final boolean upperUnbounded;
        private final boolean upperExclusive;

        DoubleRange(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive)
        {
            this.lower = lower;
            this.lowerUnbounded = lowerUnbounded;
            this.lowerExclusive = lowerExclusive;
            this.upper = upper;
            this.upperUnbounded = upperUnbounded;
            this.upperExclusive = upperExclusive;
        }

        @Override
        public boolean testDouble(double value)
        {
            if (!lowerUnbounded) {
                if (value < lower) {
                    return false;
                }
                if (lowerExclusive && lower == value) {
                    return false;
                }
            }
            if (!upperUnbounded) {
                if (value > upper) {
                    return false;
                }
                if (upperExclusive && value == upper) {
                    return false;
                }
            }
            return true;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (upper == lower) {
                return 1;
            }
            return upper != Long.MAX_VALUE && lower != Long.MIN_VALUE ? 2 : 3;
        }
    }

    public static class BytesRange
            extends Filter
    {
        private final byte[] lower;
        private final byte[] upper;
        private final boolean isEqual;
        private final boolean lowerInclusive;
        private final boolean upperInclusive;

        public BytesRange(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive)
        {
            this.lower = lower;
            this.upper = upper;
            this.lowerInclusive = lowerInclusive;
            this.upperInclusive = upperInclusive;
            isEqual = upperInclusive && lowerInclusive && Arrays.equals(upper, lower);
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            if (isEqual) {
                if (length != lower.length) {
                    return false;
                }
                for (int i = 0; i < length; i++) {
                    if (buffer[i + offset] != lower[i]) {
                        return false;
                    }
                    return true;
                }
            }
            if (lower != null) {
                int lowerCmp = memcmp(buffer, offset, length, lower, 0, lower.length);
                if (lowerCmp < 0 || (!lowerInclusive && lowerCmp == 0)) {
                    return false;
                }
            }
            if (upper != null) {
                int upperCmp = memcmp(buffer, offset, length, upper, 0, upper.length);
                return upperCmp < 0 || (upperInclusive && upperCmp == 0);
            }
            return true;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (isEqual) {
                return 5;
            }
            return upper != null && lower != null ? 6 : 7;
        }

        public boolean isEquality()
        {
            return isEqual;
        }
    }

    public static class StructFilter
            extends Filter
    {
        private final HashMap<SubfieldPath.PathElement, Filter> filters = new HashMap();

        public Filter getMember(SubfieldPath.PathElement member)
        {
            return filters.get(member);
        }

        public void addMember(SubfieldPath.PathElement member, Filter filter)
        {
            filters.put(member, filter);
        }
    }

    public static class MultiRange
        extends Filter
    {
        Filter[] filters;
        long[] longLowerBounds;
        
        MultiRange(List<Filter> filters)
        {
            this.filters = new Filter[filters.size()];
            for (int i = 0; i < this.filters.length; i++) {
                this.filters[i] = filters.get(i);
            }
            if (this.filters[0] instanceof BigintRange) {
                longLowerBounds = new long[this.filters.length];
                for (int i = 0; i < this.filters.length; i++) {
                    BigintRange range = (BigintRange) this.filters[i];
                    longLowerBounds[i] = range.getLower();
                    if (i > 0 && longLowerBounds[i] < ((BigintRange)this.filters[i - 1]).getUpper()) {
                        throw new IllegalArgumentException("Bigint filter range set must be in ascending order of lower bound and ranges must be disjoint");
                    }
                }
            }
        }

        @Override
        public boolean testLong(long value)
        {
            int i = Arrays.binarySearch(longLowerBounds, value);
            if (i >= 0) {
                return true;
            }
            int place = -1 - i;
            if (place == 0) {
                // Below first
                return false;
            }
            // When value did not hit a lower bound of a filter, test with the filter before the place where value would be inserted.
            return filters[place - 1].testLong(value);
        }

        @Override
        public boolean testDouble(double value)
        {
            for (Filter filter : filters) {
                if (filter.testDouble(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            for (Filter filter : filters) {
                if (filter.testBytes(buffer, offset, length)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class InTest
        extends Filter
    {
        static final long emptyMarker = 0xdeadbeefbadefeedL;
        static final long M = 0xc6a4a7935bd1e995L;
        private long[] longs;
        int size;
        private boolean containsEmptyMarker;

        public InTest(List<Filter> filters)
        {
            size = Integer.highestOneBit((int) (filters.size() * 3));
            longs = new long[size];
            Arrays.fill(longs, emptyMarker);
            for (Filter filter : filters) {
                long value = ((BigintRange) filter).getLower();
                if (value == emptyMarker) {
                    containsEmptyMarker = true;
                }
                else {
                    int pos = (int)((value * M) & (size - 1));
                    for (int i = pos; i < pos + size; i++) {
                        int idx = i & (size - 1);
                        if (longs[idx] == emptyMarker) {
                            longs[idx] = value;
                            break;
                        }
                    }
                }
            }
        }

        @Override
        public boolean testLong(long value)
        {
            if (containsEmptyMarker && value == emptyMarker) {
                return true;
            }
            int pos = (int) ((value * M) & (size - 1));
            for (int i = pos; i < pos + size; i++) {
                int idx = i & (size - 1);
                long l = longs[idx];
                if (l == emptyMarker) {
                    return false;
                }
                if (l == value) {
                    return true;
                }
            }
            return false;
        }
    }

    public static Filter createMultiRange(List<Filter> filters)
    {
        if (filters.get(0) instanceof BigintRange && filters.stream().allMatch(Filter::isEquality)) {
            return new InTest(filters);
        }
        else {
            return new MultiRange(filters);
        }
    }
    
}

