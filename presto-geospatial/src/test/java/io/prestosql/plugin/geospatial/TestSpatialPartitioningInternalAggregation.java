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
package io.prestosql.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.block.BlockAssertions;
import io.prestosql.geospatial.KdbTreeUtils;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.Accumulator;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.GroupedAccumulator;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.math.DoubleMath.roundToInt;
import static io.prestosql.geospatial.KdbTree.buildKdbTree;
import static io.prestosql.geospatial.serde.GeometrySerde.serialize;
import static io.prestosql.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static io.prestosql.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static io.prestosql.operator.aggregation.AggregationTestUtils.getGroupValue;
import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY;
import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.math.RoundingMode.CEILING;
import static org.testng.Assert.assertEquals;

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

        Rectangle expectedExtent = new Rectangle(-10, -10, Math.nextUp(10.0), Math.nextUp(10.0));
        String expectedValue = getSpatialPartitioning(expectedExtent, geometries, partitionCount);

        AccumulatorFactory accumulatorFactory = function.bind(Ints.asList(0, 1, 2), Optional.empty());
        Page page = new Page(geometryBlock, partitionCountBlock);

        Accumulator accumulator = accumulatorFactory.createAccumulator();
        accumulator.addInput(page);
        String aggregation = (String) BlockAssertions.getOnlyValue(accumulator.getFinalType(), getFinalBlock(accumulator));
        assertEquals(aggregation, expectedValue);

        GroupedAccumulator groupedAggregation = accumulatorFactory.createGroupedAccumulator();
        groupedAggregation.addInput(createGroupByIdBlock(0, page.getPositionCount()), page);
        String groupValue = (String) getGroupValue(groupedAggregation, 0);
        assertEquals(groupValue, expectedValue);
    }

    private InternalAggregationFunction getFunction()
    {
        return functionAssertions
                .getMetadata()
                .getFunctionRegistry()
                .getAggregateFunctionImplementation(
                        new Signature("spatial_partitioning",
                                FunctionKind.AGGREGATE,
                                TypeSignature.parseTypeSignature(VARCHAR),
                                TypeSignature.parseTypeSignature(GEOMETRY_TYPE_NAME),
                                TypeSignature.parseTypeSignature(INTEGER)));
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

    private String getSpatialPartitioning(Rectangle extent, List<OGCGeometry> geometries, int partitionCount)
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (OGCGeometry geometry : geometries) {
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);
            rectangles.add(new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
        }

        return KdbTreeUtils.toJson(buildKdbTree(roundToInt(geometries.size() * 1.0 / partitionCount, CEILING), extent, rectangles.build()));
    }
}
