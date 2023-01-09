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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestMapColumnStatistics
{
    private static final KeyInfo INT_KEY1 = KeyInfo.newBuilder().setIntKey(1).build();
    private static final KeyInfo INT_KEY2 = KeyInfo.newBuilder().setIntKey(2).build();
    private static final KeyInfo INT_KEY3 = KeyInfo.newBuilder().setIntKey(3).build();
    private static final KeyInfo STRING_KEY1 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s1")).build();
    private static final KeyInfo STRING_KEY2 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s2")).build();
    private static final KeyInfo STRING_KEY3 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s3")).build();

    @DataProvider
    public Object[][] keySupplier()
    {
        return new Object[][] {
                {INT_KEY1, INT_KEY2, INT_KEY3},
                {STRING_KEY1, STRING_KEY2, STRING_KEY3},
        };
    }

    @Test(dataProvider = "keySupplier")
    public void testEqualsHashCode(KeyInfo[] keys)
    {
        MapColumnStatisticsBuilder builder1 = new MapColumnStatisticsBuilder(true);
        builder1.addMapStatistics(keys[0], new ColumnStatistics(3L, null));
        builder1.addMapStatistics(keys[1], new ColumnStatistics(5L, null));
        builder1.increaseValueCount(8);
        ColumnStatistics columnStatistics1 = builder1.buildColumnStatistics();

        // same as builder1
        MapColumnStatisticsBuilder builder2 = new MapColumnStatisticsBuilder(true);
        builder2.addMapStatistics(keys[0], new ColumnStatistics(3L, null));
        builder2.addMapStatistics(keys[1], new ColumnStatistics(5L, null));
        builder2.increaseValueCount(8);
        ColumnStatistics columnStatistics2 = builder2.buildColumnStatistics();

        MapColumnStatisticsBuilder builder3 = new MapColumnStatisticsBuilder(true);
        builder3.addMapStatistics(keys[1], new ColumnStatistics(5L, null));
        builder3.addMapStatistics(keys[2], new ColumnStatistics(6L, null));
        builder3.increaseValueCount(11);
        ColumnStatistics columnStatistics3 = builder3.buildColumnStatistics();

        // 1 and 2 should be equal
        assertEquals(columnStatistics1, columnStatistics2);
        assertEquals(columnStatistics1.hashCode(), columnStatistics2.hashCode());

        // 2 and 3 should be not equal
        assertNotEquals(columnStatistics2, columnStatistics3);
        assertNotEquals(columnStatistics2.hashCode(), columnStatistics3.hashCode());
    }
}
