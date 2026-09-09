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
package com.facebook.presto.delta;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Subfield;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.delta.DeltaTypeUtils.toPhysicalSubfieldPath;
import static org.testng.Assert.assertEquals;

/**
 * Test {@link DeltaColumnHandle} is created with correct parameters and round trip JSON SerDe works.
 */
public class TestDeltaColumnHandle
{
    private final JsonCodec<DeltaColumnHandle> codec = JsonCodec.jsonCodec(DeltaColumnHandle.class);

    @Test
    public void testPartitionColumn()
    {
        DeltaColumnHandle expectedPartitionColumn = new DeltaColumnHandle(null, "partitionColumn", "partitionColumn", parseTypeSignature(DOUBLE), PARTITION, Optional.empty());
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testRegularColumn()
    {
        DeltaColumnHandle expectedRegularColumn = new DeltaColumnHandle(0L, "phys", "regularColumn", parseTypeSignature(DOUBLE), REGULAR, Optional.of(new Subfield("first")));
        testRoundTrip(expectedRegularColumn);
    }

    @Test
    public void testPhysicalSubfieldPath()
    {
        DeltaColumnHandle column = new DeltaColumnHandle(
                1L,
                "col-root",
                "root",
                parseTypeSignature("row(child row(value bigint))"),
                parseTypeSignature("row(\"col-child\" row(\"col-value\" bigint))"),
                REGULAR,
                Optional.empty(),
                ImmutableList.of());
        Subfield subfield = new Subfield(
                "root",
                ImmutableList.of(new Subfield.NestedField("child"), new Subfield.NestedField("value")));

        assertEquals(toPhysicalSubfieldPath(column, subfield), ImmutableList.of("col-root", "col-child", "col-value"));
    }

    private void testRoundTrip(DeltaColumnHandle expected)
    {
        String json = codec.toJson(expected);
        DeltaColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getPhysicalName(), expected.getPhysicalName());
        assertEquals(actual.getLogicalName(), expected.getLogicalName());
        assertEquals(actual.getColumnType(), expected.getColumnType());
        assertEquals(actual.getDataType(), expected.getDataType());
        assertEquals(actual.getPhysicalType(), expected.getPhysicalType());
        assertEquals(actual.getSubfield(), expected.getSubfield());
        assertEquals(actual.getSourceSubfieldPath(), expected.getSourceSubfieldPath());
    }
}
