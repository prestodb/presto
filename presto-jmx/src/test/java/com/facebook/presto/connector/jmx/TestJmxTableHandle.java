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
            .add(new JmxColumnHandle("connectorId", "id", BIGINT))
            .add(new JmxColumnHandle("connectorId", "name", createUnboundedVarcharType()))
            .build();
    public static final JmxTableHandle TABLE = new JmxTableHandle("connectorId", "objectName", COLUMNS);

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
                .addEquivalentGroup(new JmxTableHandle("connector", "name", COLUMNS), new JmxTableHandle("connector", "name", COLUMNS))
                .addEquivalentGroup(new JmxTableHandle("connectorX", "name", COLUMNS), new JmxTableHandle("connectorX", "name", COLUMNS))
                .addEquivalentGroup(new JmxTableHandle("connector", "nameX", COLUMNS), new JmxTableHandle("connector", "nameX", COLUMNS))
                .addEquivalentGroup(new JmxTableHandle("connector", "name", singleColumn), new JmxTableHandle("connector", "name", singleColumn))
                .check();
    }
}
