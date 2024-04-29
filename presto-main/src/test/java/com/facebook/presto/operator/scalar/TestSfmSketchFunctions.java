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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.google.common.io.BaseEncoding;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestSfmSketchFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testCardinality()
    {
        SfmSketch sketch = createSketch(1, 10_000, 4);
        assertEquals(SfmSketchFunctions.cardinality(sketch.serialize()), sketch.cardinality());
    }

    @Test
    public void testEmptyApproxSet()
    {
        // with no privacy (epsilon = infinity), an empty approx set should return 0 cardinality
        assertFunction("cardinality(noisy_empty_approx_set_sfm(infinity()))", BIGINT, 0L);
        assertFunction("cardinality(noisy_empty_approx_set_sfm(infinity(), 4096))", BIGINT, 0L);
        assertFunction("cardinality(noisy_empty_approx_set_sfm(infinity(), 4096, 24))", BIGINT, 0L);
    }

    @Test
    public void testCastRoundTrip()
    {
        assertFunction("cardinality(CAST(CAST(noisy_empty_approx_set_sfm(infinity()) AS VARBINARY) AS SFMSKETCH))", BIGINT, 0L);
    }

    @Test
    public void testMergeNullArray()
    {
        assertFunction("merge_sfm(ARRAY[NULL, NULL, NULL]) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testMergeEmptyArray()
    {
        // calling with an empty array should return NULL
        assertFunction("merge_sfm(ARRAY[]) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testMergeSingleArray()
    {
        // merging a single SFM sketch should simply return the sketch
        String sketchProjection = getSketchProjection(createSketch(1, 10_000, 3));
        assertFunction("cardinality(merge_sfm(ARRAY[" + sketchProjection + "])) = cardinality(" + sketchProjection + ")", BOOLEAN, true);
    }

    @Test
    public void testMergeManyArrays()
    {
        // merging many sketches should return a single merged sketch
        // (using non-private sketches here for a deterministic test)
        String sketchProjection1 = getSketchProjection(createSketch(1, 50, SfmSketch.NON_PRIVATE_EPSILON));
        String sketchProjection2 = getSketchProjection(createSketch(51, 200, SfmSketch.NON_PRIVATE_EPSILON));
        String sketchProjection3 = getSketchProjection(createSketch(100, 300, SfmSketch.NON_PRIVATE_EPSILON));
        String sketchProjectionMerged = getSketchProjection(createSketch(1, 300, SfmSketch.NON_PRIVATE_EPSILON));
        String arrayProjection = "ARRAY[" + sketchProjection1 + ", " + sketchProjection2 + ", " + sketchProjection3 + "]";
        assertFunction("CAST(merge_sfm(" + arrayProjection + ") AS VARBINARY) = CAST(" + sketchProjectionMerged + " AS VARBINARY)", BOOLEAN, true);
    }

    private SfmSketch createSketch(int start, int end, double epsilon)
    {
        SfmSketch sketch = SfmSketch.create(2048, 16);
        for (int i = start; i <= end; i++) {
            sketch.add(i);
        }

        if (epsilon < SfmSketch.NON_PRIVATE_EPSILON) {
            sketch.enablePrivacy(epsilon);
        }

        return sketch;
    }

    private String getSketchProjection(SfmSketch sketch)
    {
        byte[] binary = sketch.serialize().getBytes();
        String encoded = BaseEncoding.base16().lowerCase().encode(binary);
        return "CAST(X'" + encoded + "' AS SFMSKETCH)";
    }
}
