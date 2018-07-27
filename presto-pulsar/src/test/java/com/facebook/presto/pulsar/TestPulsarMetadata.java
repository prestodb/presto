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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.javax.ws.rs.ClientErrorException;
import org.apache.pulsar.shade.javax.ws.rs.core.Response;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public class TestPulsarMetadata extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarMetadata.class);

    @Test
    public void testListSchemaNames() {

        List<String> schemas = this.pulsarMetadata.listSchemaNames(mock(ConnectorSession.class));

        String[] expectedSchemas = {NAMESPACE_NAME_1.toString(), NAMESPACE_NAME_2.toString(),
                NAMESPACE_NAME_3.toString(), NAMESPACE_NAME_4.toString()};
        Assert.assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
    }

    @Test
    public void testGetTableHandle() {

        SchemaTableName schemaTableName = new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName());

        ConnectorTableHandle connectorTableHandle
                = this.pulsarMetadata.getTableHandle(mock(ConnectorSession.class), schemaTableName);

        Assert.assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        Assert.assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        Assert.assertEquals(pulsarTableHandle.getSchemaName(), TOPIC_1.getNamespace());
        Assert.assertEquals(pulsarTableHandle.getTableName(), TOPIC_1.getLocalName());
        Assert.assertEquals(pulsarTableHandle.getTopicName(), TOPIC_1.getLocalName());
    }

    @Test
    public void testGetTableLayouts() {

    }

    @Test
    public void testGetTableLayout() {

    }

    @Test
    public void testGetTableMetadata() {

        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames);
        allTopics.addAll(partitionedTopicNames);

        for (TopicName topic : allTopics) {
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                    topic.toString(),
                    topic.getNamespace(),
                    topic.getLocalName(),
                    topic.getLocalName()
            );

            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);

            Assert.assertEquals(tableMetadata.getTable().getSchemaName(), topic.getNamespace());
            Assert.assertEquals(tableMetadata.getTable().getTableName(), topic.getLocalName());

            Assert.assertEquals(tableMetadata.getColumns().size(),
                    Foo.class.getDeclaredFields().length + PulsarInternalColumn.getInternalFields().size());

            List<String> fieldNames = new LinkedList<>();
            for (Field field : Foo.class.getDeclaredFields()) {
                fieldNames.add(field.getName());
            }

            for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
                fieldNames.add(internalField.getName());
            }

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                    Assert.assertEquals(column.getComment(),
                            PulsarInternalColumn.getInternalFieldsMap()
                                    .get(column.getName()).getColumnMetadata(true).getComment());
                }

                fieldNames.remove(column.getName());
            }

            Assert.assertTrue(fieldNames.isEmpty());
        }
    }

    @Test
    public void testGetTableMetadataWrongSchema() {

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                "wrong-tenant/wrong-ns",
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            Assert.fail("Invalid schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            Assert.assertEquals(e.getMessage(), "Schema wrong-tenant/wrong-ns does not exist");
        }
    }

    @Test
    public void testGetTableMetadataWrongTable() {

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                "wrong-topic",
                "wrong-topic"
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            Assert.fail("Invalid table should have generated an exception");
        } catch (TableNotFoundException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            Assert.assertEquals(e.getMessage(), "Table 'tenant-1/ns-1.wrong-topic' not found");
        }
    }

    @Test
    public void testGetTableMetadataTableNoSchema() throws PulsarAdminException {

        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenThrow(
                new PulsarAdminException(new ClientErrorException(Response.Status.NOT_FOUND)));

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            Assert.fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            Assert.assertEquals(e.getMessage(), "Topic persistent://tenant-1/ns-1/topic-1 does not have a schema");
        }
    }

    @Test
    public void testGetTableMetadataTableBlankSchema() throws PulsarAdminException {

        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema(new byte[0]);
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            Assert.fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            Assert.assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

    @Test
    public void testGetTableMetadataTableInvalidSchema() throws PulsarAdminException {

        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema("foo".getBytes());
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
            Assert.fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            Assert.assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

    @Test
    public void testListTable() {
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), null).isEmpty());
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), "wrong-tenant/wrong-ns")
                .isEmpty());

        SchemaTableName[] expectedTopics1 = {new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName())};
        Assert.assertEquals(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                NAMESPACE_NAME_3.toString()), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName(TOPIC_5.getNamespace(), TOPIC_5.getLocalName()),
                new SchemaTableName(TOPIC_6.getNamespace(), TOPIC_6.getLocalName())};
        Assert.assertEquals(new HashSet<>(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                NAMESPACE_NAME_4.toString())), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test
    public void testGetColumnHandles() {

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(), TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(), TOPIC_1.getLocalName());
        Map<String, ColumnHandle> columnHandleMap
                = new HashMap<>(this.pulsarMetadata.getColumnHandles(mock(ConnectorSession.class), pulsarTableHandle));

        List<String> fieldNames = new LinkedList<>();
        for (Field field : Foo.class.getDeclaredFields()) {
            fieldNames.add(field.getName());
        }

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (String field : fieldNames) {
            Assert.assertNotNull(columnHandleMap.get(field));
            PulsarColumnHandle pulsarColumnHandle = (PulsarColumnHandle) columnHandleMap.get(field);
            PulsarInternalColumn pulsarInternalColumn = PulsarInternalColumn.getInternalFieldsMap().get(field);
            if (pulsarInternalColumn != null) {
                Assert.assertEquals(pulsarColumnHandle,
                        pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false));
            } else {
                Schema schema = new Schema.Parser().parse(new String(topicsToSchemas.get(TOPIC_1.getSchemaName())
                        .getSchema()));
                Assert.assertEquals(pulsarColumnHandle.getConnectorId(), pulsarConnectorId.toString());
                Assert.assertEquals(pulsarColumnHandle.getName(), schema.getField(field).name());
                Assert.assertNotNull(pulsarColumnHandle.getPositionIndex());
                Assert.assertEquals(pulsarColumnHandle.getPositionIndex().intValue(), schema.getField(field).pos());
                Assert.assertEquals(pulsarColumnHandle.getType(), fooTypes.get(field));
                Assert.assertEquals(pulsarColumnHandle.isHidden(), false);
            }
            columnHandleMap.remove(field);
        }
        Assert.assertTrue(columnHandleMap.isEmpty());
    }

    @Test
    public void testListTableColumns() {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_1.getNamespace()));

        Assert.assertEquals(tableColumnsMap.size(), 2);
        List<ColumnMetadata> columnMetadataList
                = tableColumnsMap.get(new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(), Foo.class.getDeclaredFields().length,
                PulsarInternalColumn.getInternalFields().size());

        List<String> fieldNames = new LinkedList<>();
        for (Field field : Foo.class.getDeclaredFields()) {
            fieldNames.add(field.getName());
        }

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                Assert.assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        Assert.assertTrue(fieldNames.isEmpty());

        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_2.getNamespace(), TOPIC_2.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(), Foo.class.getDeclaredFields().length,
                PulsarInternalColumn.getInternalFields().size());

        fieldNames = new LinkedList<>();
        for (Field field : Foo.class.getDeclaredFields()) {
            fieldNames.add(field.getName());
        }

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                Assert.assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        Assert.assertTrue(fieldNames.isEmpty());

        // test table and schema
        tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));

        Assert.assertEquals(tableColumnsMap.size(), 1);
        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(), Foo.class.getDeclaredFields().length,
                PulsarInternalColumn.getInternalFields().size());

        fieldNames = new LinkedList<>();
        for (Field field : Foo.class.getDeclaredFields()) {
            fieldNames.add(field.getName());
        }

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                Assert.assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        Assert.assertTrue(fieldNames.isEmpty());
    }
}
