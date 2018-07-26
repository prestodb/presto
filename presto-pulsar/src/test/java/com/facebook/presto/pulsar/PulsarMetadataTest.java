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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.javax.ws.rs.ClientErrorException;
import org.apache.pulsar.shade.javax.ws.rs.core.Response;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public class PulsarMetadataTest {

    private static final Logger log = Logger.get(PulsarMetadataTest.class);

    private PulsarConnectorConfig pulsarConnectorConfig;

    private PulsarMetadata pulsarMetadata;

    private PulsarAdmin pulsarAdmin;

    private Schemas schemas;

    private static List<TopicName> topicNames;
    private static List<TopicName> partitionedTopicNames;
    private static Map<String, Integer> partitionedTopicsToPartitions;
    private static Map<String, SchemaInfo> topicsToSchemas;
    private static Map<String, SchemaInfo> partitionedTopicsToSchemas;

    private static final NamespaceName NAMESPACE_NAME_1 = NamespaceName.get("tenant-1", "ns-1");
    private static final NamespaceName NAMESPACE_NAME_2 = NamespaceName.get("tenant-1", "ns-2");
    private static final NamespaceName NAMESPACE_NAME_3 = NamespaceName.get("tenant-2", "ns-1");
    private static final NamespaceName NAMESPACE_NAME_4 = NamespaceName.get("tenant-2", "ns-2");

    private static final TopicName TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-1");
    private static final TopicName TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-2");
    private static final TopicName TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2, "topic-1");
    private static final TopicName TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3, "topic-1");
    private static final TopicName TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-1");
    private static final TopicName TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-2");

    private static final TopicName PARTITIONED_TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-1");
    private static final TopicName PARTITIONED_TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-2");
    private static final TopicName PARTITIONED_TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2,
            "partitioned-topic-1");
    private static final TopicName PARTITIONED_TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3,
            "partitioned-topic-1");
    private static final TopicName PARTITIONED_TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-1");
    private static final TopicName PARTITIONED_TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-2");

    public static class Foo {
        int field1;
        String field2;
        float field3;
        double field4;
        boolean field5;
    }

    static {
        topicNames = new LinkedList<>();
        topicNames.add(TOPIC_1);
        topicNames.add(TOPIC_2);
        topicNames.add(TOPIC_3);
        topicNames.add(TOPIC_4);
        topicNames.add(TOPIC_5);
        topicNames.add(TOPIC_6);

        partitionedTopicNames = new LinkedList<>();
        partitionedTopicNames.add(PARTITIONED_TOPIC_1);
        partitionedTopicNames.add(PARTITIONED_TOPIC_2);
        partitionedTopicNames.add(PARTITIONED_TOPIC_3);
        partitionedTopicNames.add(PARTITIONED_TOPIC_4);
        partitionedTopicNames.add(PARTITIONED_TOPIC_5);
        partitionedTopicNames.add(PARTITIONED_TOPIC_6);

        partitionedTopicsToPartitions = new HashMap<>();
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_1.toString(), 2);
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_2.toString(), 3);
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_3.toString(), 4);
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_4.toString(), 5);
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_5.toString(), 6);
        partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_6.toString(), 7);

        topicsToSchemas = new HashMap<>();
        topicsToSchemas.put(TOPIC_1.getSchemaName(), AvroSchema.of(Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_2.getSchemaName(), AvroSchema.of(Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_3.getSchemaName(), AvroSchema.of(Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_4.getSchemaName(), JSONSchema.of(Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_5.getSchemaName(), JSONSchema.of(Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_6.getSchemaName(), JSONSchema.of(Foo.class).getSchemaInfo());
    }

    private final static PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");

    private static List<String> getNamespace(String tenant) {
        return new LinkedList<>(topicNames.stream().filter(new Predicate<TopicName>() {
            @Override
            public boolean test(TopicName topicName) {
                return topicName.getTenant().equals(tenant);
            }
        }).map(new Function<TopicName, String>() {
            @Override
            public String apply(TopicName topicName) {
                return topicName.getNamespace();
            }
        }).collect(Collectors.toSet()));
    }

    private static List<String> getTopics(String ns) {
        return topicNames.stream().filter(new Predicate<TopicName>() {
            @Override
            public boolean test(TopicName topicName) {
                return topicName.getNamespace().equals(ns);
            }
        }).map(new Function<TopicName, String>() {
            @Override
            public String apply(TopicName topicName) {
                return topicName.toString();
            }
        }).collect(Collectors.toList());
    }

    private static List<String> getPartitionedTopics(String ns) {
        return partitionedTopicNames.stream().filter(new Predicate<TopicName>() {
            @Override
            public boolean test(TopicName topicName) {
                return topicName.getNamespace().equals(ns);
            }
        }).map(new Function<TopicName, String>() {
            @Override
            public String apply(TopicName topicName) {
                return topicName.toString();
            }
        }).collect(Collectors.toList());
    }

    @BeforeMethod
    public void setup() throws PulsarClientException, PulsarAdminException {
        this.pulsarConnectorConfig = spy(new PulsarConnectorConfig());

        Tenants tenants = mock(Tenants.class);
        doReturn(new LinkedList<>(topicNames.stream().map(new Function<TopicName, String>() {
            @Override
            public String apply(TopicName topicName) {
                return topicName.getTenant();
            }
        }).collect(Collectors.toSet()))).when(tenants).getTenants();

        Namespaces namespaces = mock(Namespaces.class);

        when(namespaces.getNamespaces(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String tenant = (String) args[0];
                List<String> ns = getNamespace(tenant);
                if (ns.isEmpty()) {
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
                }
                return ns;
            }
        });

        Topics topics = mock(Topics.class);
        when(topics.getList(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String ns = (String) args[0];
                List<String> topics = getTopics(ns);
                if (topics.isEmpty()) {
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
                }
                return topics;
            }
        });

        when(topics.getPartitionedTopicList(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String ns = (String) args[0];
                List<String> topics = getPartitionedTopics(ns);
                if (topics.isEmpty()) {
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
                }
                return topics;
            }
        });

        when(topics.getPartitionedTopicMetadata(anyString())).thenAnswer(new Answer<PartitionedTopicMetadata>() {
            @Override
            public PartitionedTopicMetadata answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                int partitions = partitionedTopicsToPartitions.get(topic) == null
                        ? 0 : partitionedTopicsToPartitions.get(topic);
                return new PartitionedTopicMetadata(partitions);
            }
        });

        schemas = mock(Schemas.class);
        when(schemas.getSchemaInfo(anyString())).thenAnswer(new Answer<SchemaInfo>() {
            @Override
            public SchemaInfo answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                return topicsToSchemas.get(topic);
            }
        });

        pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(schemas).when(pulsarAdmin).schemas();
        doReturn(pulsarAdmin).when(this.pulsarConnectorConfig).getPulsarAdmin();

        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig);
    }

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

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                pulsarTableHandle);

        Assert.assertEquals(tableMetadata.getTable().getSchemaName(), TOPIC_1.getNamespace());
        Assert.assertEquals(tableMetadata.getTable().getTableName(), TOPIC_1.getLocalName());

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
    public void testGetTableMetadataTablePartitionedTopic() {
        
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

    }
}
