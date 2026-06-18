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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.type.SfmSketchType.SFM_SKETCH;

public class TestMergeSfmSketchAggregation
        extends AbstractTestNoisySfmAggregation
{
    protected String getFunctionName()
    {
        return "merge";
    }

    protected long getCardinalityFromResult(Object result)
    {
        return getSketchFromResult(result).cardinality();
    }

    private Block[] buildBlocks(SfmSketch... sketches)
    {
        BlockBuilder builder = SFM_SKETCH.createBlockBuilder(null, 3);
        for (SfmSketch sketch : sketches) {
            SFM_SKETCH.writeSlice(builder, sketch.serialize());
        }
        return new Block[] {builder.build()};
    }

    private void assertMergedCardinality(SfmSketch[] indivdualSketches, SfmSketch mergedSketch, double delta)
    {
        assertAggregation(
                getAggregator(getFunctionName(), SFM_SKETCH),
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                null,
                new Page(buildBlocks(indivdualSketches)),
                toSqlVarbinary(mergedSketch));
    }

    @Test
    public void testMergeOneNonPrivate()
    {
        SfmSketch sketch = createLongSketch(4096, 24, 1, 100_000);

        // deterministic test/no noise (no delta needed)
        assertMergedCardinality(new SfmSketch[] {sketch}, sketch, 0);
    }

    @Test
    public void testMergeOnePrivate()
    {
        SfmSketch sketch = createLongSketch(4096, 24, 1, 100_000);
        sketch.enablePrivacy(4);

        // although there is random noise in the sketch, the merge is a no-op, so no delta is needed
        assertMergedCardinality(new SfmSketch[] {sketch}, sketch, 0);
    }

    @Test
    public void testMergeManyNonPrivate()
    {
        SfmSketch sketch1 = createLongSketch(4096, 24, 1, 100_000);
        SfmSketch sketch2 = createLongSketch(4096, 24, 50_000, 200_000);
        SfmSketch sketch3 = createLongSketch(4096, 24, 190_000, 210_000);
        SfmSketch mergedSketch = createLongSketch(4096, 24, 1, 210_000);

        // deterministic test/no noise (no delta needed)
        assertMergedCardinality(new SfmSketch[] {sketch1, sketch2, sketch3}, mergedSketch, 0);
    }

    @Test
    public void testMergeManyPrivate()
    {
        SfmSketch sketch1 = createLongSketch(4096, 24, 1, 100_000);
        SfmSketch sketch2 = createLongSketch(4096, 24, 50_000, 200_000);
        SfmSketch sketch3 = createLongSketch(4096, 24, 190_000, 210_000);
        SfmSketch mergedSketch = createLongSketch(4096, 24, 1, 210_000);
        sketch1.enablePrivacy(10);
        sketch2.enablePrivacy(11);
        sketch3.enablePrivacy(12);

        // there is randomness in this merge, so the cardinality should only match approximately
        assertMergedCardinality(new SfmSketch[] {sketch1, sketch2, sketch3}, mergedSketch, 50_000);
    }
}
