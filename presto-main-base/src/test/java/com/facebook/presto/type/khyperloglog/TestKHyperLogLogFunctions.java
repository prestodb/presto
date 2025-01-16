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
package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class TestKHyperLogLogFunctions
        extends AbstractTestFunctions
{
    private static final int histSize = 256;
    private static final int threshold = 2;
    private static final double potential = 0.6;

    private TestKHyperLogLogFunctions() {}

    @Test
    public void testCardinalityNullArray()
    {
        assertFunction("cardinality(merge_khll(null))", BIGINT, null);
    }

    @Test
    public void testMergeSingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;

        String projection = getMergeProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential));

        functionAssertions.assertFunction("(CAST(" + projection + " AS VARBINARY)) IS NULL", BOOLEAN, false);
    }

    @Test
    public void testCardinalitySingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testReidentificationSingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;
        double error = potential * 0.05;

        String projection = getReidentificationProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential), threshold);

        functionAssertions.assertFunctionWithError(projection, DOUBLE, potential, error);
    }

    @Test
    public void testHistogramSingleColumn()
    {
        int blockSize = 1;
        long uniqueElements = 10000 * blockSize;
        double error = (potential / threshold) * 0.05;

        String projection = getHistogramProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential), histSize);

        functionAssertions.assertFunction("cardinality(" + projection + ")", BIGINT, (long) histSize);
        functionAssertions.assertFunctionWithError(projection + String.format("[%d]", threshold), DOUBLE, potential / threshold, error);
    }

    @Test
    public void testIntersectionCardinality()
    {
        int blockSize = 10;
        long uniqueElements = 10000 * blockSize;
        double error = uniqueElements * 0.05;

        List<KHyperLogLog> list1 = buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential);

        List<KHyperLogLog> list2 = buildKHyperLogLogs(15, (uniqueElements * 15) / blockSize, threshold, potential);

        String projection = getIntersectionCardinalityProjection(list1, list2);

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testJaccardIndex()
    {
        int blockSize = 10;
        long uniqueElements = 10000 * blockSize;
        double error = ((double) 2 / 3) * 0.05;

        List<KHyperLogLog> list1 = buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential);

        List<KHyperLogLog> list2 = buildKHyperLogLogs((int) (blockSize * 1.5), (int) (uniqueElements * 1.5), threshold, potential);

        String projection = getJaccardIndexProjection(list1, list2);

        functionAssertions.assertFunctionWithError(projection, DOUBLE, (double) 2 / 3, error);
    }

    @Test
    public void testMergeManyColumns()
    {
        int blockSize = 100;
        long uniqueElements = 1000 * blockSize;

        String projection = getMergeProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential));

        functionAssertions.assertFunction("(CAST(" + projection + " AS VARBINARY)) IS NULL", BOOLEAN, false);
    }

    @Test
    public void testCardinalityManyColumns()
    {
        // max number of columns to merge is 254
        int blockSize = 100;
        long uniqueElements = 1000 * blockSize;
        double error = uniqueElements * 0.05;

        String projection = getCardinalityProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential));

        functionAssertions.assertFunctionWithError(projection, BIGINT, uniqueElements, error);
    }

    @Test
    public void testReidentificationManyColumns()
    {
        int blockSize = 100;
        long uniqueElements = 1000 * blockSize;
        double error = potential * 0.05;

        String projection = getReidentificationProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential), threshold);

        functionAssertions.assertFunctionWithError(projection, DOUBLE, potential, error);
    }

    @Test
    public void testHistogramManyColumns()
    {
        int blockSize = 100;
        long uniqueElements = 1000 * blockSize;
        double error = (potential / threshold) * 0.05;

        String projection = getHistogramProjection(buildKHyperLogLogs(blockSize, uniqueElements, threshold, potential), histSize);

        functionAssertions.assertFunction("cardinality(" + projection + ")", BIGINT, (long) histSize);
        functionAssertions.assertFunctionWithError(projection + String.format("[%d]", threshold), DOUBLE, potential / threshold, error);
    }

    private List<KHyperLogLog> buildKHyperLogLogs(int blockSize, long uniqueElements, int targetThreshold, double p)
    {
        ImmutableList.Builder<KHyperLogLog> builder = ImmutableList.builder();
        Random generator = new Random(123);

        for (int j = 0; j < blockSize; j++) {
            // create a single KHyperLogLog column
            KHyperLogLog khll = new KHyperLogLog();
            // populate column with even partitions of the unique elements
            for (long i = j * uniqueElements / blockSize; i < (j + 1) * uniqueElements / blockSize; i++) {
                long baseCount = generator.nextInt(targetThreshold);
                long count = generator.nextDouble() < p ? baseCount : baseCount + targetThreshold;
                for (long uii = 0L; uii <= count; uii++) {
                    khll.add(i, uii);
                }
            }
            builder.add(khll);
        }
        return builder.build();
    }

    private String getCardinalityProjection(List<KHyperLogLog> list)
    {
        String projection = getMergeProjection(list);

        return String.format("cardinality(%s)", projection);
    }

    private String getIntersectionCardinalityProjection(List<KHyperLogLog> list1, List<KHyperLogLog> list2)
    {
        String projection1 = getMergeProjection(list1);
        String projection2 = getMergeProjection(list2);

        return String.format("intersection_cardinality(%s, %s)", projection1, projection2);
    }

    private String getJaccardIndexProjection(List<KHyperLogLog> list1, List<KHyperLogLog> list2)
    {
        String projection1 = getMergeProjection(list1);
        String projection2 = getMergeProjection(list2);

        return String.format("jaccard_index(%s, %s)", projection1, projection2);
    }

    private String getReidentificationProjection(List<KHyperLogLog> list, int threshold)
    {
        String projection = getMergeProjection(list);

        return String.format("reidentification_potential(%s, %d)", projection, threshold);
    }

    private String getHistogramProjection(List<KHyperLogLog> list, int histogramSize)
    {
        String projection = getMergeProjection(list);

        return String.format("uniqueness_distribution(%s, %d)", projection, histogramSize);
    }

    private String getMergeProjection(List<KHyperLogLog> list)
    {
        String projection = "merge_khll(ARRAY[";

        Iterator<KHyperLogLog> iterator = list.listIterator();

        ImmutableList.Builder<String> casts = ImmutableList.builder();

        for (KHyperLogLog current : list) {
            Slice firstSerial = current.serialize();

            byte[] firstBytes = firstSerial.getBytes();

            String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

            // create an iterable with all our cast statements
            casts.add("CAST(X'" + firstEncode + "' AS KHyperLogLog)");
        }

        projection += Joiner.on(", ").join(casts.build());
        projection += "])";

        return projection;
    }
}
