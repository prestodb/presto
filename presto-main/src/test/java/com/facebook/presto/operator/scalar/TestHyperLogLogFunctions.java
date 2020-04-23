package com.facebook.presto.operator.scalar;
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

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestHyperLogLogFunctions
        extends AbstractTestFunctions
{
    private TestHyperLogLogFunctions() {}

    private static final int NUMBER_OF_BUCKETS = 32768;

    @Test
    public void testCardinalityNullArray()
    {
        assertFunction("cardinality(merge_hll(null))", BIGINT, null);
    }

    @Test
    public void testCardinalityMultipleNullColumns()
    {
        assertFunction("cardinality(merge_hll(ARRAY[null, null, null]))", BIGINT, null);
    }

    @Test
    public void testMergeNoColumns()
    {
        int blockSize = 0;
        long uniqueElements = 10000 * blockSize;

        String projection = getMergeProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunction("(CAST(" + projection + " AS VARBINARY)) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testCardinalityNoColumns()
    {
        int blockSize = 0;
        long uniqueElements = 10000 * blockSize;

        String projection = getCardinalityProjection(getUniqueElements(blockSize, uniqueElements));

        assertFunction(projection, BIGINT, null);
    }

    @Test
    public void testMergeSingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getMergeProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunction("(CAST(" + projection + " AS VARBINARY)) IS NULL", BOOLEAN, false);
    }

    @Test
    public void testCardinalitySingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testCardinalityTwoColumns()
    {
        int blockSize = 2;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testCardinalityThreeColumns()
    {
        int blockSize = 3;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testMergeManyColumns()
    {
        int blockSize = 254;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getMergeProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunction("(CAST(" + projection + " AS VARBINARY)) IS NULL", BOOLEAN, false);
    }

    @Test
    public void testCardinalityManyColumns()
    {
        // max number of columns to merge is 254
        int blockSize = 254;
        long uniqueElements = 1000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(getUniqueElements(blockSize, uniqueElements));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    private List<HyperLogLog> getUniqueElements(int blockSize, long uniqueElements)
    {
        ImmutableList.Builder<HyperLogLog> builder = ImmutableList.builder();

        for (int j = 0; j < blockSize; j++) {
            // create a single HyperLogLog column
            HyperLogLog firstHll = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
            // populate column with even partitions of the unique elements
            for (long i = j * uniqueElements / blockSize; i < j * uniqueElements / blockSize +
                    uniqueElements / blockSize; i++) {
                firstHll.add(i);
            }
            builder.add(firstHll);
        }
        return builder.build();
    }

    private String getCardinalityProjection(List<HyperLogLog> list)
    {
        String projection = "cardinality(merge_hll(ARRAY[";

        Iterator<HyperLogLog> iterator = list.listIterator();

        ImmutableList.Builder<String> casts = ImmutableList.builder();

        for (HyperLogLog current : list) {
            Slice firstSerial = current.serialize();

            byte[] firstBytes = firstSerial.getBytes();

            String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

            // create an iterable with all our cast statements
            casts.add("CAST(X'" + firstEncode + "' AS HyperLogLog)");
        }

        projection += Joiner.on(", ").join(casts.build());
        projection += "]))";

        return projection;
    }

    private String getMergeProjection(List<HyperLogLog> list)
    {
        String projection = "merge_hll(ARRAY[";

        Iterator<HyperLogLog> iterator = list.listIterator();

        ImmutableList.Builder<String> casts = ImmutableList.builder();

        for (HyperLogLog current : list) {
            Slice firstSerial = current.serialize();

            byte[] firstBytes = firstSerial.getBytes();

            String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

            // create an iterable with all our cast statements
            casts.add("CAST(X'" + firstEncode + "' AS HyperLogLog)");
        }

        projection += Joiner.on(", ").join(casts.build());
        projection += "])";

        return projection;
    }
}
