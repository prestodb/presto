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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.varcharMapType;
import static com.facebook.presto.plugin.prometheus.PrometheusRecordCursor.getMapFromBlock;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.time.Instant.ofEpochMilli;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrometheusRecordSetProvider
{
    private PrometheusHttpServer prometheusHttpServer;
    private URI dataUri;
    private PrometheusClient client;

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
        dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json");
        client = new PrometheusClient(new PrometheusConnectorConfig(), METRIC_CODEC, TYPE_MANAGER);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
        }
    }

    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new PrometheusTableHandle("schema", "table");
        PrometheusRecordSetProvider recordSetProvider = new PrometheusRecordSetProvider(client);
        RecordSet recordSet = recordSetProvider.getRecordSet(
                PrometheusTransactionHandle.INSTANCE,
                SESSION,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("labels", varcharMapType, 0),
                        new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 1),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 2)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<Instant, Map<?, ?>> actual = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            actual.put((Instant) cursor.getObject(1), getMapFromBlock(varcharMapType, (Block) cursor.getObject(0)));
        }
        Map<Instant, Map<String, String>> expected = ImmutableMap.<Instant, Map<String, String>>builder()
                .put(ofEpochMilli(1565962969044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565962984045L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565962999044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565963014044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .build();
        assertEquals(actual, expected);
    }
}
