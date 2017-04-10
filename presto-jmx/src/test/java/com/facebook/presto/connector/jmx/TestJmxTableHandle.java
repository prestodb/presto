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
package com.facebook.presto.connector.jmx;

import com.google.common.collect.ImmutableList;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.connector.jmx.MetadataUtil.TABLE_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestJmxTableHandle
{
    public static final List<JmxColumnHandle> COLUMNS = ImmutableList.<JmxColumnHandle>builder()
            .add(new JmxColumnHandle("id", BIGINT))
            .add(new JmxColumnHandle("name", createUnboundedVarcharType()))
            .build();
    public static final JmxTableHandle TABLE = new JmxTableHandle("objectName", COLUMNS, true);

    @Test
    public void testJsonRoundTrip()
    {
        String json = TABLE_CODEC.toJson(TABLE);
        JmxTableHandle copy = TABLE_CODEC.fromJson(json);
        assertEquals(copy, TABLE);
    }

    @Test
    public void testEquivalence()
    {
        List<JmxColumnHandle> singleColumn = ImmutableList.of(COLUMNS.get(0));
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JmxTableHandle("name", COLUMNS, true),
                        new JmxTableHandle("name", COLUMNS, true))
                .addEquivalentGroup(
                        new JmxTableHandle("name", COLUMNS, false),
                        new JmxTableHandle("name", COLUMNS, false))
                .addEquivalentGroup(
                        new JmxTableHandle("nameX", COLUMNS, true),
                        new JmxTableHandle("nameX", COLUMNS, true))
                .addEquivalentGroup(
                        new JmxTableHandle("nameX", COLUMNS, false),
                        new JmxTableHandle("nameX", COLUMNS, false))
                .addEquivalentGroup(
                        new JmxTableHandle("name", singleColumn, true),
                        new JmxTableHandle("name", singleColumn, true))
                .addEquivalentGroup(
                        new JmxTableHandle("name", singleColumn, false),
                        new JmxTableHandle("name", singleColumn, false))
                .check();
    }
}
