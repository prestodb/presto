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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.orc.ByteArrayUtils.compareRanges;
import static com.facebook.presto.orc.ByteArrayUtils.hash;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compare;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A simple filter (e.g. comparison with literal) that can be applied efficiently
 * while extracting values from an ORC stream.
 */
public interface TupleDomainFilter
{
    TupleDomainFilter ALWAYS_FALSE = new AlwaysFalse();
    TupleDomainFilter IS_NULL = new IsNull();
    TupleDomainFilter IS_NOT_NULL = new IsNotNull();

    boolean testNull();

    boolean testLong(long value);

    boolean testDouble(double value);

    boolean testFloat(float value);

    boolean testDecimal(long low, long high);

    boolean testBoolean(boolean value);

    boolean testBytes(byte[] buffer, int offset, int length);

    abstract class AbstractTupleDomainFilter
            implements TupleDomainFilter
    {
        protected final boolean nullAllowed;

        private AbstractTupleDomainFilter(boolean nullAllowed)
        {
            this.nullAllowed = nullAllowed;
        }

        @Override
        public boolean testNull()
        {
            return nullAllowed;
        }

        @Override
        public boolean testLong(long value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testDouble(double value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testFloat(float value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }
    }

    class AlwaysFalse
            extends AbstractTupleDomainFilter
    {
        private AlwaysFalse()
        {
            super(false);
        }

        @Override
        public boolean testLong(long value)
        {
            return false;
        }

        @Override
        public boolean testDouble(double value)
        {
            return false;
        }

        @Override
        public boolean testFloat(float value)
        {
            return false;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            return false;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return false;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    class IsNull
            extends AbstractTupleDomainFilter
    {
        private IsNull()
        {
            super(true);
        }

        @Override
        public boolean testLong(long value)
        {
            return false;
        }

        @Override
        public boolean testDouble(double value)
        {
            return false;
        }

        @Override
        public boolean testFloat(float value)
        {
            return false;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            return false;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return false;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    class IsNotNull
            extends AbstractTupleDomainFilter
    {
        private IsNotNull()
        {
            super(false);
        }

        @Override
        public boolean testLong(long value)
        {
            return true;
        }

        @Override
        public boolean testDouble(double value)
        {
            return true;
        }

        @Override
        public boolean testFloat(float value)
        {
            return true;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            return true;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return true;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    class BooleanValue
            extends AbstractTupleDomainFilter
    {
        private final boolean value;

        private BooleanValue(boolean value, boolean nullAllowed)
        {
            super(nullAllowed);
            this.value = value;
        }

        public static BooleanValue of(boolean value, boolean nullAllowed)
        {
            return new BooleanValue(value, nullAllowed);
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return this.value == value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, nullAllowed);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            BooleanValue other = (BooleanValue) obj;
            return this.value == other.value &&
                    this.nullAllowed == other.nullAllowed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class BigintRange
            extends AbstractTupleDomainFilter
    {
        private final long lower;
        private final long upper;

        private BigintRange(long lower, long upper, boolean nullAllowed)
        {
            super(nullAllowed);
            checkArgument(lower <= upper, "lower must be less than or equal to upper");
            this.lower = lower;
            this.upper = upper;
        }

        public static BigintRange of(long lower, long upper, boolean nullAllowed)
        {
            return new BigintRange(lower, upper, nullAllowed);
        }

        @Override
        public boolean testLong(long value)
        {
            return value >= lower && value <= upper;
        }

        public long getLower()
        {
            return lower;
        }

        public long getUpper()
        {
            return upper;
        }

        public boolean isSingleValue()
        {
            return upper == lower;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BigintRange that = (BigintRange) o;
            return lower == that.lower &&
                    upper == that.upper &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lower, upper, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("upper", upper)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class BigintValues
            extends AbstractTupleDomainFilter
    {
        private static final long EMPTY_MARKER = 0xdeadbeefbadefeedL;
        // from Murmur hash
        private static final long M = 0xc6a4a7935bd1e995L;

        private final long[] values;
        private final long[] hashTable;
        private final int size;
        private boolean containsEmptyMarker;

        private BigintValues(long[] values, boolean nullAllowed)
        {
            super(nullAllowed);

            requireNonNull(values, "values is null");
            checkArgument(values.length > 1, "values must contain at least 2 entries");

            this.values = values;
            this.size = Integer.highestOneBit(values.length * 3);
            this.hashTable = new long[size];
            Arrays.fill(hashTable, EMPTY_MARKER);
            for (long value : values) {
                if (value == EMPTY_MARKER) {
                    containsEmptyMarker = true;
                }
                else {
                    int position = (int) ((value * M) & (size - 1));
                    for (int i = position; i < position + size; i++) {
                        int index = i & (size - 1);
                        if (hashTable[index] == EMPTY_MARKER) {
                            hashTable[index] = value;
                            break;
                        }
                    }
                }
            }
        }

        public static BigintValues of(long[] values, boolean nullAllowed)
        {
            return new BigintValues(values, nullAllowed);
        }

        @Override
        public boolean testLong(long value)
        {
            if (containsEmptyMarker && value == EMPTY_MARKER) {
                return true;
            }
            int pos = (int) ((value * M) & (size - 1));
            for (int i = pos; i < pos + size; i++) {
                int idx = i & (size - 1);
                long l = hashTable[idx];
                if (l == EMPTY_MARKER) {
                    return false;
                }
                if (l == value) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BigintValues that = (BigintValues) o;
            return size == that.size &&
                    containsEmptyMarker == that.containsEmptyMarker &&
                    Arrays.equals(hashTable, that.hashTable) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(size, containsEmptyMarker, hashTable, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("values", values)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class AbstractRange
            extends AbstractTupleDomainFilter
    {
        protected final boolean lowerUnbounded;
        protected final boolean lowerExclusive;
        protected final boolean upperUnbounded;
        protected final boolean upperExclusive;

        private AbstractRange(boolean lowerUnbounded, boolean lowerExclusive, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lowerUnbounded = lowerUnbounded;
            this.lowerExclusive = lowerExclusive;
            this.upperUnbounded = upperUnbounded;
            this.upperExclusive = upperExclusive;
        }
    }

    class DoubleRange
            extends AbstractRange
    {
        private final double lower;
        private final double upper;

        private DoubleRange(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(lowerUnbounded, lowerExclusive, upperUnbounded, upperExclusive, nullAllowed);
            this.lower = lower;
            this.upper = upper;
        }

        public static DoubleRange of(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            return new DoubleRange(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
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
        public int hashCode()
        {
            return Objects.hash(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            DoubleRange other = (DoubleRange) obj;
            return this.lower == other.lower &&
                    this.lowerUnbounded == other.lowerUnbounded &&
                    this.lowerExclusive == other.lowerExclusive &&
                    this.upper == other.upper &&
                    this.upperUnbounded == other.upperUnbounded &&
                    this.upperExclusive == other.upperExclusive &&
                    this.nullAllowed == other.nullAllowed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("lowerUnbounded", lowerUnbounded)
                    .add("lowerExclusive", lowerExclusive)
                    .add("upper", upper)
                    .add("upperUnbounded", upperUnbounded)
                    .add("upperExclusive", upperExclusive)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class FloatRange
            extends AbstractRange
    {
        private final float lower;
        private final float upper;

        private FloatRange(float lower, boolean lowerUnbounded, boolean lowerExclusive, float upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(lowerUnbounded, lowerExclusive, upperUnbounded, upperExclusive, nullAllowed);
            this.lower = lower;
            this.upper = upper;
        }

        public static FloatRange of(float lower, boolean lowerUnbounded, boolean lowerExclusive, float upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            return new FloatRange(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
        }

        @Override
        public boolean testFloat(float value)
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
        public int hashCode()
        {
            return Objects.hash(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            FloatRange other = (FloatRange) obj;
            return this.lower == other.lower &&
                    this.lowerUnbounded == other.lowerUnbounded &&
                    this.lowerExclusive == other.lowerExclusive &&
                    this.upper == other.upper &&
                    this.upperUnbounded == other.upperUnbounded &&
                    this.upperExclusive == other.upperExclusive &&
                    this.nullAllowed == other.nullAllowed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("lowerUnbounded", lowerUnbounded)
                    .add("lowerExclusive", lowerExclusive)
                    .add("upper", upper)
                    .add("upperUnbounded", upperUnbounded)
                    .add("upperExclusive", upperExclusive)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class LongDecimalRange
            extends AbstractRange
    {
        private final long lowerLow;
        private final long lowerHigh;
        private final long upperLow;
        private final long upperHigh;

        private LongDecimalRange(long lowerLow, long lowerHigh, boolean lowerUnbounded, boolean lowerExclusive, long upperLow, long upperHigh, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(lowerUnbounded, lowerExclusive, upperUnbounded, upperExclusive, nullAllowed);
            this.lowerLow = lowerLow;
            this.lowerHigh = lowerHigh;
            this.upperLow = upperLow;
            this.upperHigh = upperHigh;
        }

        public static LongDecimalRange of(long lowerLow, long lowerHigh, boolean lowerUnbounded, boolean lowerExclusive, long upperLow, long upperHigh, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            return new LongDecimalRange(lowerLow, lowerHigh, lowerUnbounded, lowerExclusive, upperLow, upperHigh, upperUnbounded, upperExclusive, nullAllowed);
        }

        @Override
        public boolean testDecimal(long valueLow, long valueHigh)
        {
            if (!lowerUnbounded) {
                int result = compare(valueLow, valueHigh, lowerLow, lowerHigh);
                if (result < 0) {
                    return false;
                }
                if (lowerExclusive && result == 0) {
                    return false;
                }
            }
            if (!upperUnbounded) {
                int result = compare(valueLow, valueHigh, upperLow, upperHigh);
                if (result > 0) {
                    return false;
                }
                if (upperExclusive && result == 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lowerLow, lowerHigh, lowerUnbounded, lowerExclusive, upperLow, upperHigh, upperUnbounded, upperExclusive, nullAllowed);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            LongDecimalRange other = (LongDecimalRange) obj;
            return this.lowerLow == other.lowerLow &&
                    this.lowerHigh == other.lowerHigh &&
                    this.lowerUnbounded == other.lowerUnbounded &&
                    this.lowerExclusive == other.lowerExclusive &&
                    this.upperLow == other.upperLow &&
                    this.upperHigh == other.upperHigh &&
                    this.upperUnbounded == other.upperUnbounded &&
                    this.upperExclusive == other.upperExclusive &&
                    this.nullAllowed == other.nullAllowed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lowerLow", lowerLow)
                    .add("lowerHigh", lowerHigh)
                    .add("lowerUnbounded", lowerUnbounded)
                    .add("lowerExclusive", lowerExclusive)
                    .add("upperLow", upperLow)
                    .add("upperHigh", upperHigh)
                    .add("upperUnbounded", upperUnbounded)
                    .add("upperExclusive", upperExclusive)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class BytesRange
            extends AbstractTupleDomainFilter
    {
        private final byte[] lower;
        private final byte[] upper;
        private final boolean lowerExclusive;
        private final boolean upperExclusive;
        private final boolean singleValue;

        private BytesRange(byte[] lower, boolean lowerExclusive, byte[] upper, boolean upperExclusive, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lower = lower;
            this.upper = upper;
            this.lowerExclusive = lowerExclusive;
            this.upperExclusive = upperExclusive;
            this.singleValue = !lowerExclusive && !upperExclusive && Arrays.equals(upper, lower);
        }

        public static BytesRange of(byte[] lower, boolean lowerExclusive, byte[] upper, boolean upperExclusive, boolean nullAllowed)
        {
            return new BytesRange(lower, lowerExclusive, upper, upperExclusive, nullAllowed);
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            if (singleValue) {
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
                int compare = compareRanges(buffer, offset, length, lower, 0, lower.length);
                if (compare < 0 || (lowerExclusive && compare == 0)) {
                    return false;
                }
            }

            if (upper != null) {
                int compare = compareRanges(buffer, offset, length, upper, 0, upper.length);
                return compare < 0 || (!upperExclusive && compare == 0);
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lower, lowerExclusive, upper, upperExclusive, nullAllowed);
        }

        public boolean isSingleValue()
        {
            return singleValue;
        }

        public byte[] getLower()
        {
            return lower;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            BytesRange other = (BytesRange) obj;
            return Arrays.equals(this.lower, other.lower) &&
                    this.lowerExclusive == other.lowerExclusive &&
                    Arrays.equals(this.upper, other.upper) &&
                    this.upperExclusive == other.upperExclusive &&
                    this.nullAllowed == other.nullAllowed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("lowerExclusive", lowerExclusive)
                    .add("upper", upper)
                    .add("upperExclusive", upperExclusive)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class BytesValues
            extends AbstractTupleDomainFilter
    {
        private final byte[][] values;
        private final byte[][] hashTable;
        private final int hashTableSizeMask;
        private final long[] bloom;
        private final int bloomSize;

        private BytesValues(byte[][] values, boolean nullAllowed)
        {
            super(nullAllowed);

            requireNonNull(values, "values is null");
            checkArgument(values.length > 1, "values must contain at least 2 entries");

            this.values = values;
            // Linear hash table size is the highest power of two less than or equal to number of values * 4. This means that the
            // table is under half full, e.g. 127 elements gets 256 slots.
            int hashTableSize = Integer.highestOneBit(values.length * 4);
            hashTableSizeMask = hashTableSize - 1;
            hashTable = new byte[hashTableSize][];
            // 8 bits of Bloom filter per slot in hash table. The bloomSize is a count of longs, hence / 8.
            bloomSize = Math.max(1, hashTableSize / 8);
            bloom = new long[bloomSize];
            for (byte[] value : values) {
                long hashCode = hash(value, 0, value.length);
                bloom[bloomIndex(hashCode)] |= bloomMask(hashCode);
                int position = (int) (hashCode & hashTableSizeMask);
                for (int i = position; i <= position + hashTableSizeMask; i++) {
                    int index = i & hashTableSizeMask;
                    if (hashTable[index] == null) {
                        hashTable[index] = value;
                        break;
                    }
                    if (compareRanges(value, 0, value.length, hashTable[index], 0, hashTable[index].length) == 0) {
                        break;
                    }
                }
            }
        }

        public static BytesValues of(byte[][] values, boolean nullAllowed)
        {
            return new BytesValues(values, nullAllowed);
        }

        @Override
        public boolean testBytes(byte[] value, int offset, int length)
        {
            long hashCode = hash(value, offset, length);
            if (!testBloom(hashCode)) {
                return false;
            }
            int position = (int) (hashCode & hashTableSizeMask);
            for (int i = position; i <= position + hashTableSizeMask; i++) {
                int index = i & hashTableSizeMask;
                byte[] entry = hashTable[index];
                if (entry == null) {
                    return false;
                }
                if (compareRanges(value, offset, length, entry, 0, entry.length) == 0) {
                    return true;
                }
            }
            return false;
        }

        private static long bloomMask(long hashCode)
        {
            return (1L << ((hashCode >> 20) & 63)) | (1L << ((hashCode >> 26) & 63)) | (1L << ((hashCode >> 32) & 63));
        }

        private int bloomIndex(long hashCode)
        {
            return (int) ((hashCode >> 38) & (bloomSize - 1));
        }

        private boolean testBloom(long hashCode)
        {
            long mask = bloomMask(hashCode);
            int index = bloomIndex(hashCode);
            return mask == (bloom[index] & mask);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BytesValues that = (BytesValues) o;
            return nullAllowed == that.nullAllowed &&
                    Arrays.deepEquals(values, that.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(values, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("values", values)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class BigintMultiRange
            extends AbstractTupleDomainFilter
    {
        private final BigintRange[] ranges;
        private final long[] longLowerBounds;

        private BigintMultiRange(List<BigintRange> ranges, boolean nullAllowed)
        {
            super(nullAllowed);
            requireNonNull(ranges, "ranges is null");
            checkArgument(!ranges.isEmpty(), "ranges is empty");

            this.ranges = ranges.toArray(new BigintRange[0]);
            this.longLowerBounds = ranges.stream()
                    .mapToLong(BigintRange::getLower)
                    .toArray();

            for (int i = 1; i < longLowerBounds.length; i++) {
                checkArgument(longLowerBounds[i] >= ranges.get(i - 1).getUpper(), "bigint ranges must not overlap");
            }
        }

        public static BigintMultiRange of(List<BigintRange> ranges, boolean nullAllowed)
        {
            return new BigintMultiRange(ranges, nullAllowed);
        }

        @Override
        public boolean testLong(long value)
        {
            int i = Arrays.binarySearch(longLowerBounds, value);
            if (i >= 0) {
                return true;
            }
            int place = (-i) - 1;
            if (place == 0) {
                // Below first
                return false;
            }
            // When value did not hit a lower bound of a filter, test with the filter before the place where value would be inserted.
            return ranges[place - 1].testLong(value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BigintMultiRange that = (BigintMultiRange) o;
            return Arrays.equals(ranges, that.ranges) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(ranges, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("ranges", ranges)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    class MultiRange
            extends AbstractTupleDomainFilter
    {
        private final TupleDomainFilter[] filters;

        private MultiRange(List<TupleDomainFilter> filters, boolean nullAllowed)
        {
            super(nullAllowed);
            requireNonNull(filters, "filters is null");
            checkArgument(filters.size() > 1, "filters must contain at least 2 entries");

            this.filters = filters.toArray(new TupleDomainFilter[0]);
        }

        public static MultiRange of(List<TupleDomainFilter> filters, boolean nullAllowed)
        {
            return new MultiRange(filters, nullAllowed);
        }

        @Override
        public boolean testDouble(double value)
        {
            for (TupleDomainFilter filter : filters) {
                if (filter.testDouble(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testFloat(float value)
        {
            for (TupleDomainFilter filter : filters) {
                if (filter.testFloat(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            for (TupleDomainFilter filter : filters) {
                if (filter.testDecimal(low, high)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            for (TupleDomainFilter filter : filters) {
                if (filter.testBytes(buffer, offset, length)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MultiRange that = (MultiRange) o;
            return Arrays.equals(filters, that.filters) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filters, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filters", filters)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }
}
