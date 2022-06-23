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
package com.facebook.presto.plugin.prometheus;

import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.TABLE_CODEC;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.varcharMapType;
import static org.testng.Assert.assertEquals;

public class TestPrometheusTable
{
    private static final Metadata METADATA = createTestMetadataManager();
    public static final TypeManager TYPE_MANAGER = METADATA.getFunctionAndTypeManager();
    private final PrometheusTable prometheusTable = new PrometheusTable("tableName",
            ImmutableList.of(
                    new PrometheusColumn("labels", varcharMapType),
                    new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                    new PrometheusColumn("value", DoubleType.DOUBLE)));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(prometheusTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("labels", varcharMapType),
                new ColumnMetadata("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                new ColumnMetadata("value", DoubleType.DOUBLE)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = TABLE_CODEC.toJson(prometheusTable);
        PrometheusTable prometheusTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(prometheusTableCopy.getName(), prometheusTable.getName());
        assertEquals(prometheusTableCopy.getColumns(), prometheusTable.getColumns());
    }
}
