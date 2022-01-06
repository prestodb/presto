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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * DictionaryRowGroupBuilder is used for building the row group indexes in DictionaryColumnWriter.
 * DictionaryRowGroupBuilder serves two purposes:
 *  1. Though row group is typically configured to be 10K rows. When encoding deeply nested columns
 *  like Array[Array[String]], the number of indexes can be in few million. Though the dictionary
 *  in most cases only contain 65,000 elements. The indexes in this case should use short (2 bytes)
 *  instead of int(4 bytes). Per a million indexes, this will save 2 MB of memory. DictionaryCompressionOptimizer
 *  while calculating bytes will use 2 bytes for dictionary index, but if stored as Int, this would
 *  take more space in memory and will cause OOM. This is a problem for configuration running with
 *  large dictionary (>50 MB), though presto default dictionary size is 16 MB and does not exhibit this  problem.
 *  2. Maintaining one large array for all indexes in the row group will result in OOM as well.
 *  DictionaryRowGroup builder will use segments to split one large index array into smaller segments.
 *
 *  The alternative of DictionaryRowGroupBuilder is to use IntBigArray which segments by default.
 *  But IntBigArray, does not dynamically use byte or short when the indexes are small. Further
 *  IntBigArrays, let you store and retrieve by offset, which is unnecessary overhead.
 *
 *  Note: ORC uses one dictionary per stripe and the dictionary size will always grow until stripe
 *  is abandoned or flushed. DictionaryRowGroupBuilder when dictionary has less than Byte.MAX_VALUE
 *  entries will use the byte segments to store and will progress to short and then finally to integer.
 *  So the users of DictionaryRowGroup should process byte segments, then short segments and then int
 *  segments. Mixing up the order will cause the entries to be processed out of order and bugs.
 *
 *  Once a row group is complete, DictionaryRowGroup(immutable) is created which resizes the segments
 *  to required length.
 */
class DictionaryRowGroupBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryRowGroupBuilder.class).instanceSize();

    private byte[][] byteSegments;
    private int byteSegmentOffset;

    private short[][] shortSegments;
    private int shortSegmentOffset;

    private int[][] integerSegments;
    private int integerSegmentOffset;

    private int indexRetainedBytes;
    private int lastMaxIndex = -1;

    private static int calculateNewLength(int expectedLength, int currentLength)
    {
        return Math.max(expectedLength + 1, (int) (currentLength * 1.5));
    }

    private void appendByteIndexes(byte[] byteIndexes)
    {
        indexRetainedBytes += sizeOf(byteIndexes);
        if (byteSegments == null) {
            byteSegments = new byte[1][];
        }
        else if (byteSegmentOffset >= byteSegments.length) {
            byteSegments = Arrays.copyOf(byteSegments, calculateNewLength(byteSegmentOffset, byteSegments.length));
        }

        byteSegments[byteSegmentOffset++] = byteIndexes;
    }

    private void appendShortIndexes(short[] shortIndexes)
    {
        indexRetainedBytes += sizeOf(shortIndexes);
        if (shortSegments == null) {
            shortSegments = new short[1][];
        }
        else if (shortSegmentOffset >= shortSegments.length) {
            shortSegments = Arrays.copyOf(shortSegments, calculateNewLength(shortSegmentOffset, shortSegments.length));
        }

        shortSegments[shortSegmentOffset++] = shortIndexes;
    }

    private void appendIntegerIndexes(int[] intIndexes)
    {
        indexRetainedBytes += sizeOf(intIndexes);
        if (integerSegments == null) {
            integerSegments = new int[1][];
        }
        else if (integerSegmentOffset >= integerSegments.length) {
            integerSegments = Arrays.copyOf(integerSegments, calculateNewLength(integerSegmentOffset, integerSegments.length));
        }

        integerSegments[integerSegmentOffset++] = intIndexes;
    }

    public void addIndexes(int maxIndex, int[] dictionaryIndexes, int indexCount)
    {
        if (indexCount == 0 && indexRetainedBytes > 0) {
           // Ignore empty segment, since there are other segments present.
            return;
        }
        checkState(maxIndex >= lastMaxIndex, "LastMax is greater than the current max");
        lastMaxIndex = maxIndex;

        if (maxIndex <= Byte.MAX_VALUE) {
            byte[] byteIndexes = new byte[indexCount];
            for (int i = 0; i < indexCount; i++) {
                byteIndexes[i] = (byte) dictionaryIndexes[i];
            }
            appendByteIndexes(byteIndexes);
        }
        else if (maxIndex <= Short.MAX_VALUE) {
            short[] shortIndexes = new short[indexCount];
            for (int i = 0; i < indexCount; i++) {
                shortIndexes[i] = (short) dictionaryIndexes[i];
            }
            appendShortIndexes(shortIndexes);
        }
        else {
            int[] intIndexes = Arrays.copyOf(dictionaryIndexes, indexCount);
            appendIntegerIndexes(intIndexes);
        }
    }

    public DictionaryRowGroup build(ColumnStatistics columnStatistics)
    {
        return new DictionaryRowGroup(getByteSegments(), getShortSegments(), getIntegerSegments(), columnStatistics);
    }

    private static <T> T[] truncateToLength(T[] original, int newLength)
    {
        if (original == null) {
            return null;
        }
        if (original.length == newLength) {
            return original;
        }
        return Arrays.copyOf(original, newLength);
    }

    @Nullable
    public byte[][] getByteSegments()
    {
        return truncateToLength(byteSegments, byteSegmentOffset);
    }

    @Nullable
    public short[][] getShortSegments()
    {
        return truncateToLength(shortSegments, shortSegmentOffset);
    }

    @Nullable
    public int[][] getIntegerSegments()
    {
        return truncateToLength(integerSegments, integerSegmentOffset);
    }

    public int getIndexRetainedBytes()
    {
        return indexRetainedBytes;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(byteSegments) +
                sizeOf(shortSegments) +
                sizeOf(integerSegments) +
                indexRetainedBytes;
    }

    public void reset()
    {
        byteSegments = null;
        byteSegmentOffset = 0;

        shortSegments = null;
        shortSegmentOffset = 0;

        integerSegments = null;
        integerSegmentOffset = 0;

        indexRetainedBytes = 0;
        lastMaxIndex = -1;
    }
}
