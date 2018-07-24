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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.javax.ws.rs.ClientErrorException;
import org.apache.pulsar.shade.javax.ws.rs.core.Response;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class PulsarMetadataTest {

    private static final Logger log = Logger.get(PulsarMetadataTest.class);

    private PulsarConnectorConfig pulsarConnectorConfig;

    private PulsarMetadata pulsarMetadata;

    private static List<TopicName> topicNames;
    static {
        topicNames = new LinkedList<>();
        topicNames.add(TopicName.get("persistent", "tenant-1", "ns-1", "topic-1"));
        topicNames.add(TopicName.get("persistent", "tenant-1", "ns-1", "topic-2"));
        topicNames.add(TopicName.get("persistent", "tenant-1", "ns-2", "topic-1"));

        topicNames.add(TopicName.get("persistent", "tenant-2", "ns-1", "topic-1"));
        topicNames.add(TopicName.get("persistent", "tenant-2", "ns-2", "topic-1"));
        topicNames.add(TopicName.get("persistent", "tenant-2", "ns-2", "topic-2"));
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
                return topicName.getLocalName();
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

        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(pulsarAdmin).when(this.pulsarConnectorConfig).getPulsarAdmin();

        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig);

    }

    @Test
    public void testListSchemaNames() {

        List<String> schemas = this.pulsarMetadata.listSchemaNames(mock(ConnectorSession.class));

        String[] expectedSchemas = {"tenant-2/ns-1", "tenant-2/ns-2", "tenant-1/ns-1", "tenant-1/ns-2"};
        Assert.assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
    }

    @Test
    public void testGetTableHandle() {

        SchemaTableName schemaTableName = new SchemaTableName("tenant-2/ns-1", "topic-1");

        ConnectorTableHandle connectorTableHandle
                = this.pulsarMetadata.getTableHandle(mock(ConnectorSession.class), schemaTableName);

        Assert.assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        Assert.assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        Assert.assertEquals(pulsarTableHandle.getSchemaName(), "tenant-2/ns-1");
        Assert.assertEquals(pulsarTableHandle.getTableName(), "topic-1");
        Assert.assertEquals(pulsarTableHandle.getTopicName(), "topic-1");
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
                "tenant-2/ns-1",
                "topic-1",
                "topic-1"
        );

        this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class), pulsarTableHandle);
    }

    @Test
    public void testListTable() {
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), null).isEmpty());
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), "wrong-tenant/wrong-ns").isEmpty());

        SchemaTableName[] expectedTopics1 = {new SchemaTableName("tenant-2/ns-1", "topic-1")};
        Assert.assertEquals(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                "tenant-2/ns-1"), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName("tenant-2/ns-2", "topic-1"),
                new SchemaTableName("tenant-2/ns-2", "topic-2")};
        Assert.assertEquals(new HashSet<>(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                "tenant-2/ns-2")), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test
    public void testGetColumnHandles() {

    }
}
