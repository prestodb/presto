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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusClient;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests against Prometheus container
 */
@Test(singleThreaded = true)
public class TestPrometheusRetrieveUpValueIntegrationTests
{
    private static final PrometheusTableHandle RUNTIME_DETERMINED_TABLE_HANDLE = new PrometheusTableHandle("default", "up");

    private PrometheusServer server;
    private PrometheusClient client;

    @BeforeClass
    protected void createQueryRunner()
    {
        this.server = new PrometheusServer();
        this.client = createPrometheusClient(server);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testRetrieveUpValue()
            throws Exception
    {
        PrometheusServer.checkServerReady(this.client);
        assertTrue(client.getTableNames("default").contains("up"), "Prometheus' own `up` metric should be available in default");
    }

    @Test(dependsOnMethods = "testRetrieveUpValue")
    public void testHandleErrorResponse()
    {
        assertThatThrownBy(() -> client.getTableNames("unknown"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Prometheus did no return metrics list (table names)");
        PrometheusTable table = client.getTable("unknown", "up");
        assertNull(table);
    }

    @Test(dependsOnMethods = "testRetrieveUpValue")
    public void testListSchemaNames()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("default"));
    }

    @Test(dependsOnMethods = "testRetrieveUpValue")
    public void testGetColumnMetadata()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        assertEquals(metadata.getColumnMetadata(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE, new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // prometheus connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // PrometheusTableHandle and PrometheusColumnHandle passed in.  This is on because
        // it is not possible for the Presto Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = PrestoException.class, dependsOnMethods = "testRetrieveUpValue")
    public void testCreateTable()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("default", "foo"),
                        ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false);
    }

    @Test(expectedExceptions = PrestoException.class, dependsOnMethods = "testRetrieveUpValue")
    public void testDropTableTable()
    {
        PrometheusMetadata metadata = new PrometheusMetadata(client);
        metadata.dropTable(SESSION, RUNTIME_DETERMINED_TABLE_HANDLE);
    }

    @Test(dependsOnMethods = "testRetrieveUpValue")
    public void testGetColumnTypes()
    {
        URI dataUri = server.getUri();
        RecordSet recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("labels", createUnboundedVarcharType(), 0),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 1),
                        new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 2)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(createUnboundedVarcharType(), DoubleType.DOUBLE, TIMESTAMP_WITH_TIME_ZONE));

        recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new PrometheusRecordSet(
                client,
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("value", BIGINT, 1),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new PrometheusRecordSet(client, new PrometheusSplit(dataUri), ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }
}
