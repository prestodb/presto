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
package com.facebook.presto.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.geospatial.KdbTreeUtils;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.geospatial.KdbTree.buildKdbTree;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.serialize;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getGroupValue;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.math.DoubleMath.roundToInt;
import static java.math.RoundingMode.CEILING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSpatialPartitioningInternalAggregation
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setup()
    {
        GeoPlugin plugin = new GeoPlugin();
        registerTypes(plugin);
        registerFunctions(plugin);
    }

    @DataProvider(name = "partitionCount")
    public static Object[][] partitionCountProvider()
    {
        return new Object[][] {{100}, {10}};
    }

    @Test(dataProvider = "partitionCount")
    public void test(int partitionCount)
    {
        InternalAggregationFunction function = getFunction();
        List<OGCGeometry> geometries = makeGeometries();
        Block geometryBlock = makeGeometryBlock(geometries);

        Block partitionCountBlock = BlockAssertions.createRLEBlock(partitionCount, geometries.size());

        String expectedValue = getSpatialPartitioning(geometries, partitionCount);

        AccumulatorFactory accumulatorFactory = function.bind(Ints.asList(0, 1), Optional.empty());
        Page page = new Page(geometryBlock, partitionCountBlock);

        Accumulator accumulator = accumulatorFactory.createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(page);
        String aggregation = (String) BlockAssertions.getOnlyValue(accumulator.getFinalType(), getFinalBlock(accumulator));
        assertEquals(aggregation, expectedValue);

        GroupedAccumulator groupedAggregation = accumulatorFactory.createGroupedAccumulator(UpdateMemory.NOOP);
        groupedAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        String groupValue = (String) getGroupValue(groupedAggregation, 0);
        assertEquals(groupValue, expectedValue);
    }

    @Test
    public void testEmptyPartitionException()
    {
        InternalAggregationFunction function = getFunction();

        Block geometryBlock = GEOMETRY.createBlockBuilder(null, 0).build();
        Block partitionCountBlock = BlockAssertions.createRLEBlock(10, 0);
        Page page = new Page(geometryBlock, partitionCountBlock);

        AccumulatorFactory accumulatorFactory = function.bind(Ints.asList(0, 1), Optional.empty());
        Accumulator accumulator = accumulatorFactory.createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(page);
        try {
            getFinalBlock(accumulator);
            fail("Should fail creating spatial partition with no rows.");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), "No rows supplied to spatial partition.");
        }
    }

    private InternalAggregationFunction getFunction()
    {
        FunctionAndTypeManager functionAndTypeManager = functionAssertions.getMetadata().getFunctionAndTypeManager();
        return functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("spatial_partitioning", fromTypes(GEOMETRY, INTEGER)));
    }

    private List<OGCGeometry> makeGeometries()
    {
        ImmutableList.Builder<OGCGeometry> geometries = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                geometries.add(new OGCPoint(new Point(-10 + i, -10 + j), null));
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                geometries.add(new OGCPoint(new Point(-10 + 2 * i, 2 * j), null));
            }
        }

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                geometries.add(new OGCPoint(new Point(2.5 * i, -10 + 2.5 * j), null));
            }
        }

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                geometries.add(new OGCPoint(new Point(5 * i, 5 * j), null));
            }
        }

        return geometries.build();
    }

    private Block makeGeometryBlock(List<OGCGeometry> geometries)
    {
        BlockBuilder builder = GEOMETRY.createBlockBuilder(null, geometries.size());
        for (OGCGeometry geometry : geometries) {
            GEOMETRY.writeSlice(builder, serialize(geometry));
        }
        return builder.build();
    }

    private String getSpatialPartitioning(List<OGCGeometry> geometries, int partitionCount)
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (OGCGeometry geometry : geometries) {
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);
            rectangles.add(new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
        }

        return KdbTreeUtils.toJson(buildKdbTree(roundToInt(geometries.size() * 1.0 / partitionCount, CEILING), rectangles.build()));
    }
}
