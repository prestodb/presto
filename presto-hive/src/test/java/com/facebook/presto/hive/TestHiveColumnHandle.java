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
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.StandardTypes;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveColumnHandle
{
    private final JsonCodec<HiveColumnHandle> codec = JsonCodec.jsonCodec(HiveColumnHandle.class);

    @Test
    public void testHiddenColumn()
    {
        HiveColumnHandle hiddenColumn = HiveColumnHandle.pathColumnHandle();
        testRoundTrip(hiddenColumn);
    }

    @Test
    public void testRowIdHiddenColumn()
    {
        HiveColumnHandle rowIdColumn = HiveColumnHandle.rowIdColumnHandle();
        testRoundTrip(rowIdColumn);
    }

    @Test
    public void testRowIdIsSynthesized()
    {
        HiveColumnHandle rowIdColumn = HiveColumnHandle.rowIdColumnHandle();
        assertEquals(rowIdColumn.getColumnType(), SYNTHESIZED);
    }

    @Test
    public void testIsRowIdColumnHandle()
    {
        HiveColumnHandle rowIdColumn = HiveColumnHandle.rowIdColumnHandle();
        assertTrue(HiveColumnHandle.isRowIdColumnHandle(rowIdColumn));
        HiveColumnHandle pathColumn = HiveColumnHandle.pathColumnHandle();
        assertFalse(HiveColumnHandle.isRowIdColumnHandle(pathColumn));
    }

    @Test
    public void testRegularColumn()
    {
        HiveColumnHandle expectedPartitionColumn = new HiveColumnHandle("name", HiveType.HIVE_FLOAT, parseTypeSignature(StandardTypes.DOUBLE), 88, PARTITION_KEY, Optional.empty(), Optional.empty());
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testPartitionKeyColumn()
    {
        HiveColumnHandle expectedRegularColumn = new HiveColumnHandle("name", HiveType.HIVE_FLOAT, parseTypeSignature(StandardTypes.DOUBLE), 88, REGULAR, Optional.empty(), Optional.empty());
        testRoundTrip(expectedRegularColumn);
    }

    private void testRoundTrip(HiveColumnHandle expected)
    {
        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getHiveType(), expected.getHiveType());
        assertEquals(actual.getHiveColumnIndex(), expected.getHiveColumnIndex());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
    }
}
