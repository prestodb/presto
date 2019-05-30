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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class TestAggregation
        extends AbstractTestAggregationFunction
{
    private static final String functionName = "differential_entropy";
    protected static final Integer SIZE = 12;
    private final String method;
    private final Double arg0;
    private final Double arg1;
    private InternalAggregationFunction aggregationFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();
        if (arg1 != null) {
            aggregationFunction = functionManager.getAggregateFunctionImplementation(
                    functionManager.lookupFunction(
                            TestAggregation.functionName,
                            fromTypes(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE, DOUBLE)));
        }
        else if (arg0 != null) {
            aggregationFunction = functionManager.getAggregateFunctionImplementation(
                    functionManager.lookupFunction(
                            TestAggregation.functionName,
                            fromTypes(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE)));
        }
        else {
            aggregationFunction = functionManager.getAggregateFunctionImplementation(
                    functionManager.lookupFunction(
                            TestAggregation.functionName,
                            fromTypes(BIGINT, DOUBLE, DOUBLE, VARCHAR)));
        }
    }

    @Test
    public void negativeSize()
    {
        try {
            assertSingleAggregation(0.0, -200, 1.0, 1.0);
            fail("Expected Exception");
        }
        catch (Exception e) {
        }
    }

    @Test
    public void negativeWeight()
    {
        try {
            assertSingleAggregation(0.0, 200, 1.0, -1.0);
            fail("Expected Exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("weight"));
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("negative"));
        }
    }

    @Test
    public void testUniform()
    {
        final int length = 99999;
        final Random random = new Random(13);
        final double min = arg0 == null ? 0.0 : arg0;
        final double max = arg1 == null ? 10.0 : arg1;
        ArrayList<Double> samples = new ArrayList<Double>();
        for (int i = 0; i < length; i++) {
            samples.add(
                    Double.valueOf(min + (max - min) * random.nextDouble()));
        }
        final double expected = Math.log(max - min) / Math.log(2);
        testSequence(samples, expected);
    }

    @Test
    public void testNormal()
    {
        final int length = 99999;
        final Random random = new Random(13);
        final double min = arg0 == null ? 0.0 : arg0;
        final double max = arg1 == null ? 10.0 : arg1;
        ArrayList<Double> samples = new ArrayList<Double>();
        for (int i = 0; i < length; i++) {
            samples.add(
                    clip((max - min) / 2 + random.nextGaussian(), (double) min, (double) max));
        }
        final double expected = 0.5 * Math.log(2 * Math.PI * Math.E) / Math.log(2);
        testSequence(samples, expected);
    }

    protected void testSequence(ArrayList<Double> samples, double expected)
    {
        final Random random = new Random(13);
        final int length = samples.size();
        BlockBuilder sizeBlockBuilder = BIGINT.createBlockBuilder(null, length);
        BlockBuilder sampleBlockBuilder = DOUBLE.createBlockBuilder(null, length);
        BlockBuilder weightBlockBuilder = DOUBLE.createBlockBuilder(null, length);
        BlockBuilder methodBlockBuilder = VARCHAR.createBlockBuilder(null, length);
        BlockBuilder arg0BlockBuilder = null;
        BlockBuilder arg1BlockBuilder = null;
        if (this.arg1 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, length);
            arg1BlockBuilder = DOUBLE.createBlockBuilder(null, length);
        }
        else if (this.arg0 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, length);
        }
        for (double s : samples) {
            BIGINT.writeLong(sizeBlockBuilder, 200);
            DOUBLE.writeDouble(sampleBlockBuilder, s);
            VARCHAR.writeSlice(methodBlockBuilder, wrappedBuffer(this.method.getBytes()));
            DOUBLE.writeDouble(weightBlockBuilder, Double.valueOf(1.0));
            if (this.arg1 != null) {
                DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
                DOUBLE.writeDouble(arg1BlockBuilder, this.arg1);
            }
            else if (this.arg0 != null) {
                DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
            }
        }
        Page page;
        if (this.arg1 != null) {
            page = new Page(
                    sizeBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build(),
                    arg1BlockBuilder.build());
        }
        else if (this.arg0 != null) {
            page = new Page(
                    sizeBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build());
        }
        else {
            page = new Page(
                    sizeBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build());
        }
        assertAggregation(
                aggregationFunction,
                (l, r) -> DoubleMath.fuzzyCompare((Double) l, (Double) r, 0.01) == 0,
                "sequence",
                page,
                expected);
    }

    private void assertSingleAggregation(double expected, long size, double sample, double weight)
    {
        if (arg1 != null) {
            assertAggregation(
                    aggregationFunction,
                    expected,
                    createLongsBlock(Long.valueOf(size)),
                    createDoublesBlock(Double.valueOf(sample)),
                    createDoublesBlock(Double.valueOf(weight)),
                    createStringsBlock(method),
                    createDoublesBlock(Double.valueOf(arg0)),
                    createDoublesBlock(Double.valueOf(arg1)));
        }
        else if (arg0 != null) {
            assertAggregation(
                    aggregationFunction,
                    expected,
                    createLongsBlock(Long.valueOf(size)),
                    createDoublesBlock(Double.valueOf(sample)),
                    createDoublesBlock(Double.valueOf(weight)),
                    createStringsBlock(method),
                    createDoublesBlock(Double.valueOf(arg0)));
        }
        else {
            assertAggregation(
                    aggregationFunction,
                    expected,
                    createLongsBlock(Long.valueOf(size)),
                    createDoublesBlock(Double.valueOf(sample)),
                    createDoublesBlock(Double.valueOf(weight)),
                    createStringsBlock(method));
        }
    }

    protected TestAggregation(String method, Double arg0, Double arg1)
    {
        this.method = method;
        this.arg0 = arg0;
        this.arg1 = arg1;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder sizeBlockBuilder = BIGINT.createBlockBuilder(null, 2 * length);
        BlockBuilder sampleBlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        BlockBuilder weightBlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        BlockBuilder methodBlockBuilder = VARCHAR.createBlockBuilder(null, 2 * length);
        BlockBuilder arg0BlockBuilder = null;
        BlockBuilder arg1BlockBuilder = null;
        if (this.arg1 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
            arg1BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        }
        else if (this.arg0 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        }
        for (int weight = 1; weight < 3; ++weight) {
            for (int i = start; i < start + length; i++) {
                BIGINT.writeLong(sizeBlockBuilder, TestAggregation.SIZE);
                DOUBLE.writeDouble(
                        sampleBlockBuilder,
                        Double.valueOf(
                                clip((double) i, 0.0, (double) TestAggregation.SIZE)));
                VARCHAR.writeSlice(methodBlockBuilder, wrappedBuffer(this.method.getBytes()));
                DOUBLE.writeDouble(weightBlockBuilder, Double.valueOf(weight));
                if (this.arg1 != null) {
                    DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
                    DOUBLE.writeDouble(arg1BlockBuilder, this.arg1);
                }
                else if (this.arg0 != null) {
                    DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
                }
            }
        }

        if (this.arg1 != null) {
            return new Block[]{
                    sizeBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build(),
                    arg1BlockBuilder.build()
            };
        }
        if (this.arg0 != null) {
            return new Block[]{
                    sizeBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build()
            };
        }
        return new Block[]{
                sizeBlockBuilder.build(),
                sampleBlockBuilder.build(),
                weightBlockBuilder.build(),
                methodBlockBuilder.build()
        };
    }

    @Override
    protected String getFunctionName()
    {
        return "differential_entropy";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        if (this.arg1 != null) {
            return ImmutableList.of(
                    StandardTypes.INTEGER,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE,
                    StandardTypes.VARCHAR,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE);
        }

        if (this.arg0 != null) {
            return ImmutableList.of(
                    StandardTypes.INTEGER,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE,
                    StandardTypes.VARCHAR,
                    StandardTypes.DOUBLE);
        }

        return ImmutableList.of(
                StandardTypes.INTEGER,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE,
                StandardTypes.VARCHAR);
    }

    protected void getSamplesAndWeights(
            int start,
            int length,
            ArrayList<Double> samples,
            ArrayList<Double> weights)
    {
        for (int weight = 1; weight < 3; ++weight) {
            for (int i = start; i < start + length; ++i) {
                final int bin = clip(i, 0, TestAggregation.SIZE - 1);
                samples.add(Double.valueOf(bin));
                weights.add(Double.valueOf(weight));
            }
        }
    }

    protected static double clip(double value, double min, double max)
    {
        return Math.min(Math.max(value, min), max);
    }

    protected static int clip(int value, int min, int max)
    {
        return Math.min(Math.max(value, min), max);
    }
}
