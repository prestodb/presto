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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
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
        DeltaColumnHandle expectedPartitionColumn = new DeltaColumnHandle("partitionColumn", parseTypeSignature(DOUBLE), PARTITION, Optional.empty());
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testRegularColumn()
    {
        DeltaColumnHandle expectedRegularColumn = new DeltaColumnHandle("regularColumn", parseTypeSignature(DOUBLE), REGULAR, Optional.of(new Subfield("first")));
        testRoundTrip(expectedRegularColumn);
    }

    private void testRoundTrip(DeltaColumnHandle expected)
    {
        String json = codec.toJson(expected);
        DeltaColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getColumnType(), expected.getColumnType());
        assertEquals(actual.getDataType(), expected.getDataType());
        assertEquals(actual.getSubfield(), expected.getSubfield());
    }
}
