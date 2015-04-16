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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.SqlDate;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;

public class TestArrayAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testEmpty()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getExactFunction(new Signature("array_agg", "array<bigint>", "bigint")).getAggregationFunction();
        assertAggregation(
                bigIntAgg,
                1.0,
                null,
                new Page(createLongsBlock(new Long[] {})));
    }

    @Test
    public void testNullOnly()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getExactFunction(new Signature("array_agg", "array<bigint>", "bigint")).getAggregationFunction();
        assertAggregation(
                bigIntAgg,
                1.0,
                null,
                new Page(createLongsBlock(new Long[] {null, null, null})));
    }

    @Test
    public void testNullPartial()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getExactFunction(new Signature("array_agg", "array<bigint>", "bigint")).getAggregationFunction();
        assertAggregation(
                bigIntAgg,
                1.0,
                Arrays.asList(2L, 3L),
                new Page(createLongsBlock(new Long[] {null, 2L, null, 3L, null})));
    }

    @Test
    public void testBoolean()
        throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getExactFunction(new Signature("array_agg", "array<boolean>", "boolean")).getAggregationFunction();
        assertAggregation(
                booleanAgg,
                1.0,
                Arrays.asList(true, false),
                new Page(createBooleansBlock(new Boolean[] {true, false})));
    }

    @Test
    public void testBigInt()
        throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getExactFunction(new Signature("array_agg", "array<bigint>", "bigint")).getAggregationFunction();
        assertAggregation(
                bigIntAgg,
                1.0,
                Arrays.asList(2L, 1L, 2L),
                new Page(createLongsBlock(new Long[] {2L, 1L, 2L})));
    }

    @Test
    public void testVarchar()
        throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getExactFunction(new Signature("array_agg", "array<varchar>", "varchar")).getAggregationFunction();
        assertAggregation(
                varcharAgg,
                1.0,
                Arrays.asList("hello", "world"),
                new Page(createStringsBlock(new String[] {"hello", "world"})));
    }

    @Test
    public void testDate()
            throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getExactFunction(new Signature("array_agg", "array<date>", "date")).getAggregationFunction();
        assertAggregation(
                varcharAgg,
                1.0,
                Arrays.asList(new SqlDate(1), new SqlDate(2), new SqlDate(4)),
                new Page(createLongsBlock(new Long[] {1L, 2L, 4L})));
    }

    @Test
    public void testArray()
            throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getExactFunction(new Signature("array_agg", "array<array<bigint>>", "array<bigint>")).getAggregationFunction();

        BlockBuilder builder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 100);

        BlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(variableWidthBlockBuilder, 1);
        VARBINARY.writeSlice(builder, buildStructuralSlice(variableWidthBlockBuilder));

        variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(variableWidthBlockBuilder, 1);
        BIGINT.writeLong(variableWidthBlockBuilder, 2);
        VARBINARY.writeSlice(builder, buildStructuralSlice(variableWidthBlockBuilder));

        variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(variableWidthBlockBuilder, 1);
        BIGINT.writeLong(variableWidthBlockBuilder, 2);
        BIGINT.writeLong(variableWidthBlockBuilder, 3);
        VARBINARY.writeSlice(builder, buildStructuralSlice(variableWidthBlockBuilder));

        assertAggregation(
                varcharAgg,
                1.0,
                Arrays.asList(Arrays.asList(1L), Arrays.asList(1L, 2L), Arrays.asList(1L, 2L, 3L)),
                new Page(builder.build()));
    }
}
