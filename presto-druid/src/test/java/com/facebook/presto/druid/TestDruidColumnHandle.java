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
package com.facebook.presto.druid;

import com.facebook.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.druid.DruidColumnHandle.DruidColumnType.DERIVED;
import static com.facebook.presto.druid.DruidColumnHandle.DruidColumnType.REGULAR;
import static com.facebook.presto.druid.TestingMetadataUtil.COLUMN_CODEC;
import static org.testng.Assert.assertEquals;

public class TestDruidColumnHandle
{
    private final DruidColumnHandle columnHandle = new DruidColumnHandle("columnName", VARCHAR, REGULAR);

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        DruidColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester
                .equivalenceTester()
                .addEquivalentGroup(
                        new DruidColumnHandle("columnName", BIGINT, REGULAR),
                        new DruidColumnHandle("columnName", BIGINT, DERIVED))
                .addEquivalentGroup(
                        new DruidColumnHandle("columnNameX", VARCHAR, REGULAR),
                        new DruidColumnHandle("columnNameX", VARCHAR, DERIVED))
                .check();
    }
}
