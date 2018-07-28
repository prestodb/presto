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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.airlift.log.Logger;
import javafx.util.Pair;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.pulsar.shade.javax.ws.rs.ClientErrorException;
import org.apache.pulsar.shade.javax.ws.rs.core.Response;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static org.apache.pulsar.common.api.Commands.serializeMetadataAndPayload;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public abstract class TestPulsarConnector {

    protected PulsarConnectorConfig pulsarConnectorConfig;

    protected PulsarMetadata pulsarMetadata;

    protected PulsarAdmin pulsarAdmin;

    protected Schemas schemas;

    protected PulsarSplitManager pulsarSplitManager;

    protected Map<TopicName, PulsarRecordCursor> pulsarRecordCursors = new HashMap<>();

    protected final static PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");

    protected static List<TopicName> topicNames;
    protected static List<TopicName> partitionedTopicNames;
    protected static Map<String, Integer> partitionedTopicsToPartitions;
    protected static Map<String, SchemaInfo> topicsToSchemas;
    protected static Map<String, Long> topicsToEntries;

    protected static final NamespaceName NAMESPACE_NAME_1 = NamespaceName.get("tenant-1", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_2 = NamespaceName.get("tenant-1", "ns-2");
    protected static final NamespaceName NAMESPACE_NAME_3 = NamespaceName.get("tenant-2", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_4 = NamespaceName.get("tenant-2", "ns-2");

    protected static final TopicName TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-1");
    protected static final TopicName TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-2");
    protected static final TopicName TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2, "topic-1");
    protected static final TopicName TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3, "topic-1");
    protected static final TopicName TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-1");
    protected static final TopicName TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-2");

    protected static final TopicName PARTITIONED_TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-2");
    protected static final TopicName PARTITIONED_TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-2");

    public static class Foo {
        int field1;
        String field2;
        float field3;
        double field4;
        boolean field5;
        long field6;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
        protected long timestamp;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"time-millis\" }")
        protected int time;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        protected int date;

        public int getField1() {
            return field1;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public float getField3() {
            return field3;
        }

        public void setField3(float field3) {
            this.field3 = field3;
        }

        public double getField4() {
            return field4;
        }

        public void setField4(double field4) {
            this.field4 = field4;
        }

        public boolean isField5() {
            return field5;
        }

        public void setField5(boolean field5) {
            this.field5 = field5;
        }

        public long getField6() {
            return field6;
        }

        public void setField6(long field6) {
            this.field6 = field6;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getTime() {
            return time;
        }

        public void setTime(int time) {
            this.time = time;
        }

        public int getDate() {
            return date;
        }

        public void setDate(int date) {
            this.date = date;
        }
    }

    protected static Map<String, Type> fooTypes;
    protected static List<PulsarColumnHandle> fooColumnHandles;
    protected static Map<TopicName, PulsarSplit> splits;

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
        topicsToSchemas.put(TOPIC_1.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_2.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_3.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_4.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_5.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(TOPIC_6.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());


        topicsToSchemas.put(PARTITIONED_TOPIC_1.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(PARTITIONED_TOPIC_2.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(PARTITIONED_TOPIC_3.getSchemaName(), AvroSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(PARTITIONED_TOPIC_4.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(PARTITIONED_TOPIC_5.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());
        topicsToSchemas.put(PARTITIONED_TOPIC_6.getSchemaName(), JSONSchema.of(TestPulsarMetadata.Foo.class).getSchemaInfo());

        fooTypes = new HashMap<>();
        fooTypes.put("field1", IntegerType.INTEGER);
        fooTypes.put("field2", VarcharType.VARCHAR);
        fooTypes.put("field3", RealType.REAL);
        fooTypes.put("field4", DoubleType.DOUBLE);
        fooTypes.put("field5", BooleanType.BOOLEAN);
        fooTypes.put("field6", BigintType.BIGINT);
        fooTypes.put("timestamp", TIMESTAMP);
        fooTypes.put("time", TIME);
        fooTypes.put("date", DATE);

        topicsToEntries = new HashMap<>();
        topicsToEntries.put(TOPIC_1.getSchemaName(), 1233L);
        topicsToEntries.put(TOPIC_2.getSchemaName(), 0L);
        topicsToEntries.put(TOPIC_3.getSchemaName(), 100L);
        topicsToEntries.put(TOPIC_4.getSchemaName(), 12345L);
        topicsToEntries.put(TOPIC_5.getSchemaName(), 8000L);
        topicsToEntries.put(TOPIC_6.getSchemaName(), 1L);
        topicsToEntries.put(PARTITIONED_TOPIC_1.getSchemaName(), 1233L);
        topicsToEntries.put(PARTITIONED_TOPIC_2.getSchemaName(), 8000L);
        topicsToEntries.put(PARTITIONED_TOPIC_3.getSchemaName(), 100L);
        topicsToEntries.put(PARTITIONED_TOPIC_4.getSchemaName(), 0L);
        topicsToEntries.put(PARTITIONED_TOPIC_5.getSchemaName(), 800L);
        topicsToEntries.put(PARTITIONED_TOPIC_6.getSchemaName(), 1L);

        fooColumnHandles = new LinkedList<>();
        for (int i = 0; i < Foo.class.getDeclaredFields().length; i++) {
            Field field = Foo.class.getDeclaredFields()[i];
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    field.getName(),
                    fooTypes.get(field.getName()),
                    false,
                    false,
                    i));
        }
        fooColumnHandles.addAll(PulsarInternalColumn.getInternalFields().stream().map(
                new Function<PulsarInternalColumn, PulsarColumnHandle>() {
                    @Override
                    public PulsarColumnHandle apply(PulsarInternalColumn pulsarInternalColumn) {
                        return pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false);
                    }
                }).collect(Collectors.toList()));

        splits = new HashMap<>();

        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames);
        allTopics.addAll(partitionedTopicNames);

        for (TopicName topicName : allTopics) {
            splits.put(topicName, new PulsarSplit(0, pulsarConnectorId.toString(),
                    topicName.getNamespace(), topicName.getLocalName(),
                    topicsToEntries.get(topicName.getSchemaName()),
                    new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()),
                    topicsToSchemas.get(topicName.getSchemaName()).getType(),
                    0, topicsToEntries.get(topicName.getSchemaName()),
                    0, 0));
        }
    }

    private static final Logger log = Logger.get(TestPulsarConnector.class);

    protected static List<String> getNamespace(String tenant) {
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

    protected static List<String> getTopics(String ns) {
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

    protected static List<String> getPartitionedTopics(String ns) {
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
    public void setup() throws Exception {
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
        this.pulsarSplitManager = spy(new PulsarSplitManager(pulsarConnectorId, this.pulsarConnectorConfig));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        when(managedLedgerFactory.openReadOnlyCursor(any(), any(), any())).then(new Answer<ReadOnlyCursor>() {

            private Map<String, Integer> positions = new HashMap<>();

            private int count = 0;
            @Override
            public ReadOnlyCursor answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                positions.put(topic, 0);
                String schemaName = TopicName.get(
                        TopicName.get(
                                topic.replaceAll("/persistent", ""))
                                .getPartitionedTopicName()).getSchemaName();
                long entries = topicsToEntries.get(schemaName);
                ReadOnlyCursor readOnlyCursor = mock(ReadOnlyCursor.class);
                doReturn(entries).when(readOnlyCursor).getNumberOfEntries();

                doAnswer(new Answer<Void>() {
                    @Override
                    public Void answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        Integer skipEntries = (Integer) args[0];
                        positions.put(topic, positions.get(topic) + skipEntries);
                        return null;
                    }
                }).when(readOnlyCursor).skipEntries(anyInt());

                when(readOnlyCursor.getReadPosition()).thenAnswer(new Answer<PositionImpl>() {
                    @Override
                    public PositionImpl answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return PositionImpl.get(0, positions.get(topic));
                    }
                });

                when(readOnlyCursor.readEntries(anyInt())).thenAnswer(new Answer<List<Entry>>() {
                    @Override
                    public List<Entry> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Object[] args = invocationOnMock.getArguments();
                        Integer readEntries = (Integer) args[0];

                        List<Entry> entries = new LinkedList<>();
                        for (int i = 0; i < readEntries; i++) {

                            Foo foo = new Foo();
                            foo.field1 = count;
                            foo.field2 = String.valueOf(count);
                            foo.field3 = count;
                            foo.field4 = count;
                            foo.field5 = count % 2 == 0;
                            foo.field6 = count;
                            foo.timestamp = System.currentTimeMillis();

                            LocalTime now = LocalTime.now(ZoneId.systemDefault());
                            foo.time = now.toSecondOfDay() * 1000;

                            LocalDate localDate = LocalDate.now();
                            LocalDate epoch = LocalDate.ofEpochDay(0);
                            foo.date = Math.toIntExact(ChronoUnit.DAYS.between(epoch, localDate));

                            PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                                    .setProducerName("test-producer").setSequenceId(positions.get(topic))
                                    .setPublishTime(System.currentTimeMillis()).build();

                            Schema schema = topicsToSchemas.get(schemaName).getType() == SchemaType.AVRO ? AvroSchema.of(Foo.class) : JSONSchema.of(Foo.class);

                            org.apache.pulsar.shade.io.netty.buffer.ByteBuf payload
                                    = org.apache.pulsar.shade.io.netty.buffer.Unpooled.copiedBuffer(schema.encode(foo));

                            ByteBuf byteBuf = serializeMetadataAndPayload
                                    (Commands.ChecksumType.Crc32c, messageMetadata, payload);

                            entries.add(EntryImpl.create(0, positions.get(topic), byteBuf));
                            positions.put(topic, positions.get(topic) + 1);
                            count++;
                        }

                        return entries;
                    }
                });

                when(readOnlyCursor.hasMoreEntries()).thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return positions.get(topic) < entries;
                    }
                });

                return readOnlyCursor;
            }
        });

        doReturn(managedLedgerFactory).when(this.pulsarSplitManager).getManagedLedgerFactory();

        for (Map.Entry<TopicName, PulsarSplit> split : splits.entrySet()) {

            PulsarRecordCursor pulsarRecordCursor = spy(new PulsarRecordCursor(fooColumnHandles, split.getValue(), pulsarConnectorConfig, managedLedgerFactory));
            this.pulsarRecordCursors.put(split.getKey(), pulsarRecordCursor);
        }
    }

    @AfterMethod
    public void cleanup() {

    }
}
