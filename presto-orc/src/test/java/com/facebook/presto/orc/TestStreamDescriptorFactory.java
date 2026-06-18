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
package com.facebook.presto.orc;

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.StreamDescriptorFactory.AllStreams;
import com.facebook.presto.orc.StreamDescriptorFactory.StreamProperty;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.StreamDescriptorFactory.createStreamDescriptor;
import static com.facebook.presto.orc.metadata.OrcType.toOrcType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestStreamDescriptorFactory
{
    private static final OrcDataSource DUMMY_ORC_DATA_SOURCE = new NoopOrcDataSource();

    private static void verifyStreamDescriptor(StreamDescriptor streamDescriptor, int expectedStreamId, int expectedSequence, Map<Integer, StreamProperty> streamPropertyMap)
    {
        assertEquals(streamDescriptor.getStreamId(), expectedStreamId, "streamId");
        assertEquals(streamDescriptor.getSequence(), expectedSequence, "sequence");

        assertEquals(streamDescriptor.getOrcDataSource(), DUMMY_ORC_DATA_SOURCE, "sequence");

        StreamProperty streamProperty = streamPropertyMap.get(expectedStreamId);
        assertEquals(streamDescriptor.getStreamName(), streamProperty.getStreamName(), "stream name");
        assertEquals(streamDescriptor.getFieldName(), streamProperty.getFieldName(), "field name");
        assertEquals(streamDescriptor.getOrcType(), streamProperty.getOrcType(), "orc type");

        List<StreamDescriptor> nestedStreamDescriptors = streamDescriptor.getNestedStreams();
        assertEquals(nestedStreamDescriptors.size(), streamProperty.getNestedStreamIds().size(), "nested streams for stream Id " + expectedStreamId);
        for (int i = 0; i < nestedStreamDescriptors.size(); i++) {
            verifyStreamDescriptor(nestedStreamDescriptors.get(i), streamProperty.getNestedStreamIds().get(i), expectedSequence, streamPropertyMap);
        }
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }

    private static List<OrcType> getOrcTypes()
    {
        // Create schema with 2 columns
        // 1st column with name column1 and type Long
        // 2nd column with name column2 and type Map{ Long : List<Struct{inner1: Float, inner2: VARCHAR}}
        RowType.Field inner1 = new RowType.Field(Optional.of("inner1"), REAL);
        RowType.Field inner2 = new RowType.Field(Optional.of("inner2"), VARCHAR);
        RowType rowType = RowType.from(ImmutableList.of(inner1, inner2));

        Type arrayType = arrayType(rowType);
        Type mapType = mapType(BIGINT, arrayType);

        RowType.Field column1 = new RowType.Field(Optional.of("column1"), BIGINT);
        RowType.Field column2 = new RowType.Field(Optional.of("column2"), mapType);
        RowType rootType = RowType.from(ImmutableList.of(column1, column2));
        return toOrcType(0, rootType);
    }

    @Test
    public void testBuilder()
    {
        List<OrcType> orcTypes = getOrcTypes();
        StreamDescriptor streamDescriptor = createStreamDescriptor(orcTypes, DUMMY_ORC_DATA_SOURCE);

        StreamProperty rootProperty = new StreamProperty("", orcTypes.get(0), "", ImmutableList.of(1, 2));
        StreamProperty column1Property = new StreamProperty(".column1", orcTypes.get(1), "column1", ImmutableList.of());
        StreamProperty column2Property = new StreamProperty(".column2", orcTypes.get(2), "column2", ImmutableList.of(3, 4));
        StreamProperty mapKeyProperty = new StreamProperty(".column2.key", orcTypes.get(3), "key", ImmutableList.of());
        StreamProperty mapValueProperty = new StreamProperty(".column2.value", orcTypes.get(4), "value", ImmutableList.of(5));
        StreamProperty listElementProperty = new StreamProperty(".column2.value.item", orcTypes.get(5), "item", ImmutableList.of(6, 7));
        StreamProperty inner1Property = new StreamProperty(".column2.value.item.inner1", orcTypes.get(6), "inner1", ImmutableList.of());
        StreamProperty inner2Property = new StreamProperty(".column2.value.item.inner2", orcTypes.get(7), "inner2", ImmutableList.of());

        ImmutableMap.Builder<Integer, StreamProperty> streamToPropertyMapBuilder = ImmutableMap.builder();
        streamToPropertyMapBuilder.put(0, rootProperty);
        streamToPropertyMapBuilder.put(1, column1Property);
        streamToPropertyMapBuilder.put(2, column2Property);
        streamToPropertyMapBuilder.put(3, mapKeyProperty);
        streamToPropertyMapBuilder.put(4, mapValueProperty);
        streamToPropertyMapBuilder.put(5, listElementProperty);
        streamToPropertyMapBuilder.put(6, inner1Property);
        streamToPropertyMapBuilder.put(7, inner2Property);

        Map<Integer, StreamProperty> streamPropertyMap = streamToPropertyMapBuilder.build();
        verifyStreamDescriptor(streamDescriptor, 0, 0, streamPropertyMap);

        StreamDescriptor sequenceStreamDescriptor = streamDescriptor.duplicate(10);
        verifyStreamDescriptor(sequenceStreamDescriptor, 0, 10, streamPropertyMap);
    }

    @Test
    public void testAllStreamsNonExistent()
    {
        OrcType varcharType = toOrcType(0, VARCHAR).get(0);
        StreamProperty streamProperty = new StreamProperty("", varcharType, "", ImmutableList.of(1, 2));
        ImmutableMap<Integer, StreamProperty> streamIdToPropertyMap = ImmutableMap.of(0, streamProperty);
        AllStreams allStreams = new AllStreams(DUMMY_ORC_DATA_SOURCE, streamIdToPropertyMap);

        StreamProperty retrieved = allStreams.getStreamProperty(0);
        assertEquals(retrieved, streamProperty, "streamProperty");
        expectThrows(IllegalStateException.class, () -> allStreams.getStreamProperty(1));
    }
}
