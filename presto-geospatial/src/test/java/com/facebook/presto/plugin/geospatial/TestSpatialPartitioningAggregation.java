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
import com.facebook.presto.geospatial.KDBTreeBuilder;
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static java.lang.String.format;

public class TestSpatialPartitioningAggregation
        extends AbstractTestAggregationFunction
{
    @BeforeClass
    public void setup()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            typeRegistry.addType(type);
        }
        functionRegistry.addFunctions(extractFunctions(plugin.getFunctions()));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder geometryBlockBuilder = GEOMETRY.createBlockBuilder(null, length);
        BlockBuilder percentBlockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            //GEOMETRY.writeSlice(geometryBlockBuilder, stGeometryFromText(Slices.utf8Slice(format("LINESTRING (%s %s, %s %s)", start, start, i, i))));
            GEOMETRY.writeSlice(geometryBlockBuilder, stGeometryFromText(Slices.utf8Slice(format("POINT (%s %s)", i, i))));
            DoubleType.DOUBLE.writeDouble(percentBlockBuilder, 100);
        }
        return new Block[] {geometryBlockBuilder.build(), percentBlockBuilder};
    }

    @Override
    protected String getFunctionName()
    {
        return "spatial_partitioning";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(GEOMETRY_TYPE_NAME, DOUBLE);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        KDBTreeBuilder kdbTreeBuilder = new KDBTreeBuilder(1, 333, new Envelope(start, start, start + length - 1, start + length - 1));
        for (int i = start; i < start + length; i++) {
            kdbTreeBuilder.insert(new Envelope(i, i, i, i));
        }

        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        try {
            return objectMapper.writer().writeValueAsString(kdbTreeBuilder.build());
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
