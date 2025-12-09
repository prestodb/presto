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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.airlift.jaxrs.thrift.ThriftMapper;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.codec.utils.LocaleToLanguageTagCodec;
import com.facebook.presto.SessionTestUtils;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.TaskTestUtils;
import com.facebook.presto.execution.TestQueryManager;
import com.facebook.presto.execution.TestSqlTaskManager;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.ColumnHandleJacksonModule;
import com.facebook.presto.metadata.DeleteTableHandle;
import com.facebook.presto.metadata.DeleteTableHandleJacksonModule;
import com.facebook.presto.metadata.DistributedProcedureHandleJacksonModule;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionHandleJacksonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.InsertTableHandleJacksonModule;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MergeTableHandleJacksonModule;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.OutputTableHandleJacksonModule;
import com.facebook.presto.metadata.PartitioningHandleJacksonModule;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.SplitJacksonModule;
import com.facebook.presto.metadata.TableHandleJacksonModule;
import com.facebook.presto.metadata.TableLayoutHandleJacksonModule;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.thrift.ConnectorSplitThriftCodec;
import com.facebook.presto.server.thrift.DeleteTableHandleThriftCodec;
import com.facebook.presto.server.thrift.InsertTableHandleThriftCodec;
import com.facebook.presto.server.thrift.MergeTableHandleThriftCodec;
import com.facebook.presto.server.thrift.OutputTableHandleThriftCodec;
import com.facebook.presto.server.thrift.TableHandleThriftCodec;
import com.facebook.presto.server.thrift.TableLayoutHandleThriftCodec;
import com.facebook.presto.server.thrift.TransactionHandleThriftCodec;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;
import static com.facebook.presto.execution.Lifespan.driverGroup;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.server.remotetask.TestHttpRemoteTaskWithEventLoop.TestingTaskResource;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHttpRemoteTaskConnectorCodec
{
    private static final TaskManagerConfig TASK_MANAGER_CONFIG = new TaskManagerConfig();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test(timeOut = 50000)
    public void testConnectorSplitBinarySerialization()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorName = "test-codec-split";
        Injector injector = createInjectorWithCodec(connectorName, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<TaskUpdateRequest> jsonCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);
        try {
            testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
            remoteTask.start();

            Lifespan lifespan = driverGroup(1);
            TestConnectorWithCodecSplit codecSplit = new TestConnectorWithCodecSplit("test-data", 42);
            remoteTask.addSplits(ImmutableMultimap.of(
                    TaskTestUtils.TABLE_SCAN_NODE_ID,
                    new Split(new ConnectorId(connectorName), TestingTransactionHandle.create(), codecSplit, lifespan, NON_CACHEABLE)));

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getTaskSource(TaskTestUtils.TABLE_SCAN_NODE_ID) != null);
            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getTaskSource(TaskTestUtils.TABLE_SCAN_NODE_ID).getSplits().size() == 1);

            TaskUpdateRequest taskUpdateRequest = testingTaskResource.getLastTaskUpdateRequest();
            assertNotNull(taskUpdateRequest, "TaskUpdateRequest should not be null");

            String json = jsonCodec.toJson(taskUpdateRequest);
            JsonNode root = OBJECT_MAPPER.readTree(json);
            JsonNode splitNode = root.at("/sources/0/splits/0/split/connectorSplit");
            assertTrue(splitNode.has("customSerializedValue"),
                    "Split should have customSerializedValue for binary serialization");
            assertFalse(splitNode.has("data"),
                    "Split should not have inline data field");

            TaskUpdateRequest deserializedRequest = jsonCodec.fromJson(json);
            TaskSource deserializedSource = deserializedRequest.getSources().get(0);
            Split deserializedSplit = getOnlyElement(deserializedSource.getSplits()).getSplit();
            ConnectorSplit deserializedConnectorSplit = deserializedSplit.getConnectorSplit();
            assertEquals(deserializedConnectorSplit, codecSplit, "Expected deserialized split to match original");
        }
        finally {
            remoteTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    @Test(timeOut = 50000)
    public void testOutputTableHandleBinarySerialization()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorName = "test-codec-output";
        Injector injector = createInjectorWithCodec(connectorName, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        ConnectorId connectorId = new ConnectorId(connectorName);
        TestConnectorOutputTableHandle outputHandle = new TestConnectorOutputTableHandle("output_table");
        TableWriteInfo outputTableWriteInfo = new TableWriteInfo(
                Optional.of(new ExecutionWriterTarget.CreateHandle(
                        new OutputTableHandle(connectorId, com.facebook.presto.testing.TestingTransactionHandle.create(), outputHandle),
                        new SchemaTableName("test_schema", "output_table"))),
                Optional.empty());

        RemoteTask outputTask = createRemoteTask(httpRemoteTaskFactory, createPlanFragment(), outputTableWriteInfo);
        try {
            testingTaskResource.setInitialTaskInfo(outputTask.getTaskInfo());
            outputTask.start();

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getLastTaskUpdateRequest() != null);
            TaskUpdateRequest outputRequest = testingTaskResource.getLastTaskUpdateRequest();
            String outputJson = taskUpdateRequestCodec.toJson(outputRequest);

            JsonNode root = OBJECT_MAPPER.readTree(outputJson);
            JsonNode outputTableHandleNode = root.at("/tableWriteInfo/writerTarget/handle/connectorHandle");
            assertTrue(outputTableHandleNode.has("customSerializedValue"),
                    "OutputTableHandle should have customSerializedValue for binary serialization");
            assertFalse(outputTableHandleNode.has("tableName"),
                    "OutputTableHandle should not have inline tableName field");

            ExecutionWriterTarget.CreateHandle createHandle = (ExecutionWriterTarget.CreateHandle) outputRequest.getTableWriteInfo().get().getWriterTarget().get();
            TestConnectorOutputTableHandle receivedHandle = (TestConnectorOutputTableHandle) createHandle.getHandle().getConnectorHandle();
            assertEquals(receivedHandle.getTableName(), outputHandle.getTableName(), "OutputTableHandle should match after round-trip");
        }
        finally {
            outputTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    @Test(timeOut = 50000)
    public void testInsertTableHandleBinarySerialization()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorName = "test-codec-insert";
        Injector injector = createInjectorWithCodec(connectorName, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        ConnectorId connectorId = new ConnectorId(connectorName);
        TestConnectorInsertTableHandle insertHandle = new TestConnectorInsertTableHandle("insert_table");
        TableWriteInfo insertTableWriteInfo = new TableWriteInfo(
                Optional.of(new ExecutionWriterTarget.InsertHandle(
                        new InsertTableHandle(connectorId, com.facebook.presto.testing.TestingTransactionHandle.create(), insertHandle),
                        new SchemaTableName("test_schema", "insert_table"))),
                Optional.empty());

        RemoteTask insertTask = createRemoteTask(httpRemoteTaskFactory, createPlanFragment(), insertTableWriteInfo);
        try {
            testingTaskResource.setInitialTaskInfo(insertTask.getTaskInfo());
            insertTask.start();

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getLastTaskUpdateRequest() != null);
            TaskUpdateRequest insertRequest = testingTaskResource.getLastTaskUpdateRequest();
            String insertJson = taskUpdateRequestCodec.toJson(insertRequest);

            JsonNode root = OBJECT_MAPPER.readTree(insertJson);
            JsonNode insertTableHandleNode = root.at("/tableWriteInfo/writerTarget/handle/connectorHandle");
            assertTrue(insertTableHandleNode.has("customSerializedValue"),
                    "InsertTableHandle should have customSerializedValue for binary serialization");
            assertFalse(insertTableHandleNode.has("tableName"),
                    "InsertTableHandle should not have inline tableName field");

            ExecutionWriterTarget.InsertHandle deserializedInsertHandle = (ExecutionWriterTarget.InsertHandle) insertRequest.getTableWriteInfo().get().getWriterTarget().get();
            TestConnectorInsertTableHandle receivedHandle = (TestConnectorInsertTableHandle) deserializedInsertHandle.getHandle().getConnectorHandle();
            assertEquals(receivedHandle.getTableName(), insertHandle.getTableName(), "InsertTableHandle should match after round-trip");
        }
        finally {
            insertTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    @Test(timeOut = 50000)
    public void testDeleteTableHandleBinarySerialization()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorName = "test-codec-delete";
        Injector injector = createInjectorWithCodec(connectorName, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        ConnectorId connectorId = new ConnectorId(connectorName);
        TestConnectorDeleteTableHandle deleteHandle = new TestConnectorDeleteTableHandle("delete_table");
        TableWriteInfo deleteTableWriteInfo = new TableWriteInfo(
                Optional.of(new ExecutionWriterTarget.DeleteHandle(
                        new DeleteTableHandle(connectorId, com.facebook.presto.testing.TestingTransactionHandle.create(), deleteHandle),
                        new SchemaTableName("test_schema", "delete_table"))),
                Optional.empty());

        RemoteTask deleteTask = createRemoteTask(httpRemoteTaskFactory, createPlanFragment(), deleteTableWriteInfo);
        try {
            testingTaskResource.setInitialTaskInfo(deleteTask.getTaskInfo());
            deleteTask.start();

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getLastTaskUpdateRequest() != null);
            TaskUpdateRequest deleteRequest = testingTaskResource.getLastTaskUpdateRequest();
            String deleteJson = taskUpdateRequestCodec.toJson(deleteRequest);

            JsonNode root = OBJECT_MAPPER.readTree(deleteJson);
            JsonNode deleteTableHandleNode = root.at("/tableWriteInfo/writerTarget/handle/connectorHandle");
            assertTrue(deleteTableHandleNode.has("customSerializedValue"),
                    "DeleteTableHandle should have customSerializedValue for binary serialization");
            assertFalse(deleteTableHandleNode.has("tableName"),
                    "DeleteTableHandle should not have inline tableName field");

            ExecutionWriterTarget.DeleteHandle deserializedDeleteHandle = (ExecutionWriterTarget.DeleteHandle) deleteRequest.getTableWriteInfo().get().getWriterTarget().get();
            TestConnectorDeleteTableHandle receivedHandle = (TestConnectorDeleteTableHandle) deserializedDeleteHandle.getHandle().getConnectorHandle();
            assertEquals(receivedHandle.getTableName(), deleteHandle.getTableName(), "DeleteTableHandle should match after round-trip");
        }
        finally {
            deleteTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    @Test(timeOut = 50000)
    public void testConnectorHandlesBinarySerialization()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorName = "test-codec-handles";
        Injector injector = createInjectorWithCodec(connectorName, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<PlanFragment> planFragmentCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        PlanFragment planFragment = createPlanFragmentWithCodecHandles(connectorName);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, planFragment);

        try {
            testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
            remoteTask.start();

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getLastTaskUpdateRequest() != null);
            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getLastTaskUpdateRequest().getFragment().isPresent());

            TaskUpdateRequest taskUpdateRequest = testingTaskResource.getLastTaskUpdateRequest();
            byte[] fragmentBytes = taskUpdateRequest.getFragment().get();
            String json = new String(fragmentBytes, UTF_8);
            JsonNode root = OBJECT_MAPPER.readTree(json);

            JsonNode tableHandleNode = root.at("/root/table/connectorHandle");
            assertTrue(tableHandleNode.has("customSerializedValue"),
                    "TableHandle should have customSerializedValue for binary serialization");
            assertFalse(tableHandleNode.has("tableName"),
                    "TableHandle should not have inline tableName field");

            JsonNode layoutHandleNode = root.at("/root/table/connectorTableLayout");
            assertTrue(layoutHandleNode.has("customSerializedValue"),
                    "TableLayoutHandle should have customSerializedValue for binary serialization");
            assertFalse(layoutHandleNode.has("layoutName"),
                    "TableLayoutHandle should not have inline layoutName field");

            JsonNode assignmentsNode = root.at("/root/assignments");
            assertTrue(assignmentsNode.isObject() && assignmentsNode.size() > 0,
                    "Should have at least one column assignment");
            assignmentsNode.fields().forEachRemaining(entry -> {
                JsonNode columnHandleNode = entry.getValue();
                assertTrue(columnHandleNode.has("customSerializedValue"),
                        "ColumnHandle should have customSerializedValue for binary serialization");
                assertFalse(columnHandleNode.has("columnName"),
                        "ColumnHandle should not have inline columnName field");
                assertFalse(columnHandleNode.has("columnType"),
                        "ColumnHandle should not have inline columnType field");
            });

            PlanFragment receivedFragment = planFragmentCodec.fromJson(json);
            assertNotNull(receivedFragment, "Deserialized PlanFragment should not be null");
            assertNotNull(receivedFragment.getRoot(), "Deserialized PlanFragment should have a root node");

            TableScanNode originalScan = (TableScanNode) planFragment.getRoot();
            TableScanNode receivedScan = (TableScanNode) receivedFragment.getRoot();

            TestConnectorTableHandle originalTableHandle = (TestConnectorTableHandle) originalScan.getTable().getConnectorHandle();
            TestConnectorTableHandle receivedTableHandle = (TestConnectorTableHandle) receivedScan.getTable().getConnectorHandle();
            assertEquals(receivedTableHandle.getTableName(), originalTableHandle.getTableName(), "TableHandle should match after round-trip");

            TestConnectorTableLayoutHandle originalLayoutHandle = (TestConnectorTableLayoutHandle) originalScan.getTable().getLayout().get();
            TestConnectorTableLayoutHandle receivedLayoutHandle = (TestConnectorTableLayoutHandle) receivedScan.getTable().getLayout().get();
            assertEquals(receivedLayoutHandle.getLayoutName(), originalLayoutHandle.getLayoutName(), "TableLayoutHandle should match after round-trip");

            TestConnectorColumnHandle originalColumnHandle = (TestConnectorColumnHandle) originalScan.getAssignments().values().iterator().next();
            TestConnectorColumnHandle receivedColumnHandle = (TestConnectorColumnHandle) receivedScan.getAssignments().values().iterator().next();
            assertEquals(receivedColumnHandle.getColumnName(), originalColumnHandle.getColumnName(), "ColumnHandle name should match after round-trip");
            assertEquals(receivedColumnHandle.getColumnType(), originalColumnHandle.getColumnType(), "ColumnHandle type should match after round-trip");
        }
        finally {
            remoteTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    @Test(timeOut = 50000)
    public void testMixedConnectorSerializationWithAndWithoutCodec()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, TestHttpRemoteTaskWithEventLoop.FailureScenario.NO_FAILURE);

        String connectorWithCodec = "test-with-codec";
        String connectorWithoutCodec = "test-without-codec";
        Injector injector = createInjectorWithMixedConnectors(connectorWithCodec, connectorWithoutCodec, testingTaskResource);
        HttpRemoteTaskFactory httpRemoteTaskFactory = injector.getInstance(HttpRemoteTaskFactory.class);
        JsonCodec<TaskUpdateRequest> jsonCodec = injector.getInstance(Key.get(new TypeLiteral<>() {}));

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);
        try {
            testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
            remoteTask.start();

            Lifespan lifespan = driverGroup(1);

            TestConnectorWithCodecSplit splitWithCodec = new TestConnectorWithCodecSplit("codec-data", 100);
            TestConnectorWithoutCodecSplit splitWithoutCodec = new TestConnectorWithoutCodecSplit("json-data", 200);

            remoteTask.addSplits(ImmutableMultimap.of(
                    TaskTestUtils.TABLE_SCAN_NODE_ID,
                    new Split(new ConnectorId(connectorWithCodec), TestingTransactionHandle.create(), splitWithCodec, lifespan, NON_CACHEABLE),
                    TaskTestUtils.TABLE_SCAN_NODE_ID,
                    new Split(new ConnectorId(connectorWithoutCodec), TestingTransactionHandle.create(), splitWithoutCodec, lifespan, NON_CACHEABLE)));

            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getTaskSource(TaskTestUtils.TABLE_SCAN_NODE_ID) != null);
            TestHttpRemoteTaskWithEventLoop.poll(() -> testingTaskResource.getTaskSource(TaskTestUtils.TABLE_SCAN_NODE_ID).getSplits().size() == 2);

            TaskUpdateRequest taskUpdateRequest = testingTaskResource.getLastTaskUpdateRequest();
            assertNotNull(taskUpdateRequest, "TaskUpdateRequest should not be null");

            String json = jsonCodec.toJson(taskUpdateRequest);
            JsonNode root = OBJECT_MAPPER.readTree(json);
            JsonNode splitsNode = root.at("/sources/0/splits");

            assertTrue(splitsNode.isArray() && splitsNode.size() == 2,
                    "Should have exactly 2 splits");

            JsonNode codecSplitNode = null;
            JsonNode jsonSplitNode = null;

            for (JsonNode splitWrapper : splitsNode) {
                JsonNode connectorIdNode = splitWrapper.at("/split/connectorId");
                String catalogName = connectorIdNode.asText();
                JsonNode connectorSplitNode = splitWrapper.at("/split/connectorSplit");

                if (connectorWithCodec.equals(catalogName)) {
                    codecSplitNode = connectorSplitNode;
                }
                else if (connectorWithoutCodec.equals(catalogName)) {
                    jsonSplitNode = connectorSplitNode;
                }
            }

            assertNotNull(codecSplitNode, "Should find split from connector with codec");
            assertNotNull(jsonSplitNode, "Should find split from connector without codec");

            assertTrue(codecSplitNode.has("customSerializedValue"),
                    "Split with codec should have customSerializedValue for binary serialization");
            assertFalse(codecSplitNode.has("data"),
                    "Split with codec should not have inline data field");

            assertFalse(jsonSplitNode.has("customSerializedValue"),
                    "Split without codec should not have customSerializedValue");
            assertTrue(jsonSplitNode.has("data"),
                    "Split without codec should have inline data field for JSON serialization");
            assertTrue(jsonSplitNode.has("sequence"),
                    "Split without codec should have inline sequence field for JSON serialization");

            TaskUpdateRequest deserializedRequest = jsonCodec.fromJson(json);
            TaskSource deserializedSource = deserializedRequest.getSources().get(0);
            List<Split> deserializedSplits = deserializedSource.getSplits().stream()
                    .map(ScheduledSplit::getSplit)
                    .collect(toImmutableList());

            assertEquals(deserializedSplits.size(), 2, "Should have 2 deserialized splits");

            boolean foundCodecSplit = false;
            boolean foundJsonSplit = false;

            for (Split split : deserializedSplits) {
                if (split.getConnectorSplit() instanceof TestConnectorWithCodecSplit) {
                    TestConnectorWithCodecSplit deserialized = (TestConnectorWithCodecSplit) split.getConnectorSplit();
                    assertEquals(deserialized, splitWithCodec, "Codec split should match after round-trip");
                    foundCodecSplit = true;
                }
                else if (split.getConnectorSplit() instanceof TestConnectorWithoutCodecSplit) {
                    TestConnectorWithoutCodecSplit deserialized = (TestConnectorWithoutCodecSplit) split.getConnectorSplit();
                    assertEquals(deserialized, splitWithoutCodec, "JSON split should match after round-trip");
                    foundJsonSplit = true;
                }
            }

            assertTrue(foundCodecSplit, "Should have found and verified the codec split");
            assertTrue(foundJsonSplit, "Should have found and verified the JSON split");
        }
        finally {
            remoteTask.cancel();
            httpRemoteTaskFactory.stop();
        }
    }

    private static RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory)
    {
        return createRemoteTask(httpRemoteTaskFactory, createPlanFragment());
    }

    private static RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory, PlanFragment planFragment)
    {
        return createRemoteTask(httpRemoteTaskFactory, planFragment, new TableWriteInfo(Optional.empty(), Optional.empty()));
    }

    private static RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory, PlanFragment planFragment, TableWriteInfo tableWriteInfo)
    {
        return httpRemoteTaskFactory.createRemoteTask(
                SessionTestUtils.TEST_SESSION,
                new TaskId("test", 1, 0, 2, 0),
                new InternalNode("node-id", URI.create("http://fake.invalid/"), new NodeVersion("version"), false),
                planFragment,
                ImmutableMultimap.of(),
                createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST),
                new NodeTaskMap.NodeStatsTracker(i -> {}, i -> {}, (age, i) -> {}),
                true,
                tableWriteInfo,
                SchedulerStatsTracker.NOOP);
    }

    private static PlanFragment createPlanFragmentWithCodecHandles(String connectorName)
    {
        ConnectorId connectorId = new ConnectorId(connectorName);
        TestConnectorTableHandle tableHandle = new TestConnectorTableHandle("test_table");
        TestConnectorTableLayoutHandle layoutHandle = new TestConnectorTableLayoutHandle("test_layout");
        TestConnectorColumnHandle columnHandle = new TestConnectorColumnHandle("test_column", "VARCHAR");
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "test_column", com.facebook.presto.common.type.VarcharType.VARCHAR);

        return new PlanFragment(
                new PlanFragmentId(0),
                new TableScanNode(
                        Optional.empty(),
                        TaskTestUtils.TABLE_SCAN_NODE_ID,
                        new TableHandle(connectorId, tableHandle, TestingTransactionHandle.create(), Optional.of(layoutHandle)),
                        ImmutableList.of(variable),
                        ImmutableMap.of(variable, columnHandle),
                        TupleDomain.all(),
                        TupleDomain.all(),
                        Optional.empty()),
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TaskTestUtils.TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable))
                        .withBucketToPartition(Optional.of(new int[1])),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }

    private static Injector createInjectorWithCodec(String connectorName, TestingTaskResource testingTaskResource)
            throws Exception
    {
        return createInjectorWithMixedConnectors(connectorName, "unused-connector", testingTaskResource);
    }

    private static Injector createInjectorWithMixedConnectors(
            String connectorWithCodec,
            String connectorWithoutCodec,
            TestingTaskResource testingTaskResource)
            throws Exception
    {
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig().setThriftTransportEnabled(false);
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new SmileModule(),
                new ThriftCodecModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(JsonMapper.class);
                        binder.bind(ThriftMapper.class);

                        FeaturesConfig featuresConfig = new FeaturesConfig();
                        featuresConfig.setUseConnectorProvidedSerializationCodecs(true);
                        binder.bind(FeaturesConfig.class).toInstance(featuresConfig);

                        TestConnectorWithCodecProvider codecProvider = new TestConnectorWithCodecProvider();

                        Map<String, ConnectorCodec<ConnectorSplit>> splitCodecMap = new ConcurrentHashMap<>();
                        splitCodecMap.put(connectorWithCodec, codecProvider.getConnectorSplitCodec().get());

                        Map<String, ConnectorCodec<ConnectorTableHandle>> tableHandleCodecMap = new ConcurrentHashMap<>();
                        tableHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorTableHandleCodec().get());

                        Map<String, ConnectorCodec<ColumnHandle>> columnHandleCodecMap = new ConcurrentHashMap<>();
                        columnHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorColumnHandleCodec().get());

                        Map<String, ConnectorCodec<ConnectorTableLayoutHandle>> tableLayoutHandleCodecMap = new ConcurrentHashMap<>();
                        tableLayoutHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorTableLayoutHandleCodec().get());

                        Map<String, ConnectorCodec<ConnectorOutputTableHandle>> outputTableHandleCodecMap = new ConcurrentHashMap<>();
                        outputTableHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorOutputTableHandleCodec().get());

                        Map<String, ConnectorCodec<ConnectorInsertTableHandle>> insertTableHandleCodecMap = new ConcurrentHashMap<>();
                        insertTableHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorInsertTableHandleCodec().get());

                        Map<String, ConnectorCodec<ConnectorDeleteTableHandle>> deleteTableHandleCodecMap = new ConcurrentHashMap<>();
                        deleteTableHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorDeleteTableHandleCodec().get());

                        Map<String, ConnectorCodec<ConnectorMergeTableHandle>> mergeTableHandleCodecMap = new ConcurrentHashMap<>();
                        mergeTableHandleCodecMap.put(connectorWithCodec, codecProvider.getConnectorMergeTableHandleCodec().get());

                        HandleResolver handleResolver = new HandleResolver();
                        handleResolver.addConnectorName(connectorWithCodec, new TestConnectorWithCodecHandleResolver());
                        handleResolver.addConnectorName(connectorWithoutCodec, new TestConnectorWithoutCodecHandleResolver());
                        binder.bind(HandleResolver.class).toInstance(handleResolver);

                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorTableHandle>>> tableHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(tableHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorTableLayoutHandle>>> tableLayoutHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(tableLayoutHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ColumnHandle>>> columnHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(columnHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorOutputTableHandle>>> outputTableHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(outputTableHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorInsertTableHandle>>> insertTableHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(insertTableHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorDeleteTableHandle>>> deleteTableHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(deleteTableHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorMergeTableHandle>>> mergeTableHandleCodecExtractor =
                                connectorId -> Optional.ofNullable(mergeTableHandleCodecMap.get(connectorId.getCatalogName()));
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorIndexHandle>>> noOpIndexCodec =
                                connectorId -> Optional.empty();
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorTransactionHandle>>> noOpTransactionCodec =
                                connectorId -> Optional.empty();
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorPartitioningHandle>>> noOpPartitioningCodec =
                                connectorId -> Optional.empty();
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorDistributedProcedureHandle>>> noOpDistributedProcedureCodec =
                                connectorId -> Optional.empty();
                        Function<ConnectorId, Optional<ConnectorCodec<ConnectorSplit>>> splitCodecExtractor =
                                connectorId -> Optional.ofNullable(splitCodecMap.get(connectorId.getCatalogName()));

                        jsonBinder(binder).addModuleBinding().toInstance(new TableHandleJacksonModule(handleResolver, featuresConfig, tableHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new TableLayoutHandleJacksonModule(handleResolver, featuresConfig, tableLayoutHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new ColumnHandleJacksonModule(handleResolver, featuresConfig, columnHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new OutputTableHandleJacksonModule(handleResolver, featuresConfig, outputTableHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new InsertTableHandleJacksonModule(handleResolver, featuresConfig, insertTableHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new DeleteTableHandleJacksonModule(handleResolver, featuresConfig, deleteTableHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new MergeTableHandleJacksonModule(handleResolver, featuresConfig, mergeTableHandleCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new com.facebook.presto.index.IndexHandleJacksonModule(handleResolver, featuresConfig, noOpIndexCodec));
                        jsonBinder(binder).addModuleBinding().toInstance(new TransactionHandleJacksonModule(handleResolver, featuresConfig, noOpTransactionCodec));
                        jsonBinder(binder).addModuleBinding().toInstance(new PartitioningHandleJacksonModule(handleResolver, featuresConfig, noOpPartitioningCodec));
                        jsonBinder(binder).addModuleBinding().toInstance(new FunctionHandleJacksonModule(handleResolver));
                        jsonBinder(binder).addModuleBinding().toInstance(new SplitJacksonModule(handleResolver, featuresConfig, splitCodecExtractor));
                        jsonBinder(binder).addModuleBinding().toInstance(new DistributedProcedureHandleJacksonModule(handleResolver, featuresConfig, noOpDistributedProcedureCodec));

                        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
                        binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                        newSetBinder(binder, Type.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskStatus.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskInfo.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskUpdateRequest.class);
                        smileCodecBinder(binder).bindSmileCodec(PlanFragment.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                        jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
                        jsonCodecBinder(binder).bindJsonCodec(TableWriteInfo.class);
                        jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
                        jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTransactionHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ColumnHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorOutputTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorDeleteTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorInsertTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableLayoutHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorMergeTableHandle.class);

                        binder.bind(ConnectorCodecManager.class).in(Scopes.SINGLETON);

                        thriftCodecBinder(binder).bindCustomThriftCodec(ConnectorSplitThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TransactionHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(OutputTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(InsertTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DeleteTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(MergeTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TableLayoutHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskStatus.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskInfo.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskUpdateRequest.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(LocaleToLanguageTagCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(JodaDateTimeToEpochMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DurationToMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DataSizeToBytesThriftCodec.class);
                    }

                    @Provides
                    private HttpRemoteTaskFactory createHttpRemoteTaskFactory(
                            JsonMapper jsonMapper,
                            ThriftMapper thriftMapper,
                            JsonCodec<TaskStatus> taskStatusJsonCodec,
                            SmileCodec<TaskStatus> taskStatusSmileCodec,
                            ThriftCodec<TaskStatus> taskStatusThriftCodec,
                            JsonCodec<TaskInfo> taskInfoJsonCodec,
                            ThriftCodec<TaskInfo> taskInfoThriftCodec,
                            SmileCodec<TaskInfo> taskInfoSmileCodec,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
                            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
                            ThriftCodec<TaskUpdateRequest> taskUpdateRequestThriftCodec,
                            JsonCodec<PlanFragment> planFragmentJsonCodec,
                            SmileCodec<PlanFragment> planFragmentSmileCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper, thriftMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(false));
                        testingTaskResource.setHttpClient(testingHttpClient);
                        return new HttpRemoteTaskFactory(
                                new QueryManagerConfig(),
                                TASK_MANAGER_CONFIG,
                                testingHttpClient,
                                new TestSqlTaskManager.MockLocationFactory(),
                                taskStatusJsonCodec,
                                taskStatusSmileCodec,
                                taskStatusThriftCodec,
                                taskInfoJsonCodec,
                                taskInfoSmileCodec,
                                taskInfoThriftCodec,
                                taskUpdateRequestJsonCodec,
                                taskUpdateRequestSmileCodec,
                                taskUpdateRequestThriftCodec,
                                planFragmentJsonCodec,
                                planFragmentSmileCodec,
                                new RemoteTaskStats(),
                                internalCommunicationConfig,
                                createTestMetadataManager(),
                                new TestQueryManager(),
                                new HandleResolver());
                    }
                });
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("test", new com.facebook.presto.testing.TestingHandleResolver());

        ConnectorCodecManager codecManager = injector.getInstance(ConnectorCodecManager.class);
        codecManager.addConnectorCodecProvider(new ConnectorId(connectorWithCodec), new TestConnectorWithCodecProvider());

        return injector;
    }

    /**
     * Test connector split that supports binary serialization via codec
     */
    public static class TestConnectorWithCodecSplit
            implements ConnectorSplit
    {
        private final String data;
        private final int sequence;

        @JsonCreator
        public TestConnectorWithCodecSplit(
                @JsonProperty("data") String data,
                @JsonProperty("sequence") int sequence)
        {
            this.data = data;
            this.sequence = sequence;
        }

        @JsonProperty
        public String getData()
        {
            return data;
        }

        @JsonProperty
        public int getSequence()
        {
            return sequence;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NodeSelectionStrategy.NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return ImmutableMap.of("data", data, "sequence", sequence);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestConnectorWithCodecSplit that = (TestConnectorWithCodecSplit) obj;
            return sequence == that.sequence && Objects.equals(data, that.data);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(data, sequence);
        }
    }

    /**
     * Test connector codec provider that provides binary serialization
     */
    public static class TestConnectorWithCodecProvider
            implements ConnectorCodecProvider
    {
        @Override
        public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorSplit split)
                {
                    TestConnectorWithCodecSplit codecSplit = (TestConnectorWithCodecSplit) split;
                    return (codecSplit.getData() + "|" + codecSplit.getSequence()).getBytes(UTF_8);
                }

                @Override
                public ConnectorSplit deserialize(byte[] data)
                {
                    String[] parts = new String(data, UTF_8).split("\\|");
                    return new TestConnectorWithCodecSplit(parts[0], Integer.parseInt(parts[1]));
                }
            });
        }

        @Override
        public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorTableHandle handle)
                {
                    TestConnectorTableHandle tableHandle = (TestConnectorTableHandle) handle;
                    return tableHandle.getTableName().getBytes(UTF_8);
                }

                @Override
                public ConnectorTableHandle deserialize(byte[] data)
                {
                    return new TestConnectorTableHandle(new String(data, UTF_8));
                }
            });
        }

        public Optional<ConnectorCodec<ColumnHandle>> getConnectorColumnHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ColumnHandle handle)
                {
                    TestConnectorColumnHandle columnHandle = (TestConnectorColumnHandle) handle;
                    return (columnHandle.getColumnName() + ":" + columnHandle.getColumnType()).getBytes(UTF_8);
                }

                @Override
                public ColumnHandle deserialize(byte[] data)
                {
                    String[] parts = new String(data, UTF_8).split(":");
                    return new TestConnectorColumnHandle(parts[0], parts[1]);
                }
            });
        }

        public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorTableLayoutHandle handle)
                {
                    TestConnectorTableLayoutHandle layoutHandle = (TestConnectorTableLayoutHandle) handle;
                    return layoutHandle.getLayoutName().getBytes(UTF_8);
                }

                @Override
                public ConnectorTableLayoutHandle deserialize(byte[] data)
                {
                    return new TestConnectorTableLayoutHandle(new String(data, UTF_8));
                }
            });
        }

        public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorOutputTableHandle handle)
                {
                    TestConnectorOutputTableHandle outputHandle = (TestConnectorOutputTableHandle) handle;
                    return outputHandle.getTableName().getBytes(UTF_8);
                }

                @Override
                public ConnectorOutputTableHandle deserialize(byte[] data)
                {
                    return new TestConnectorOutputTableHandle(new String(data, UTF_8));
                }
            });
        }

        public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorInsertTableHandle handle)
                {
                    TestConnectorInsertTableHandle insertHandle = (TestConnectorInsertTableHandle) handle;
                    return insertHandle.getTableName().getBytes(UTF_8);
                }

                @Override
                public ConnectorInsertTableHandle deserialize(byte[] data)
                {
                    return new TestConnectorInsertTableHandle(new String(data, UTF_8));
                }
            });
        }

        public Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorDeleteTableHandle handle)
                {
                    TestConnectorDeleteTableHandle deleteHandle = (TestConnectorDeleteTableHandle) handle;
                    return deleteHandle.getTableName().getBytes(UTF_8);
                }

                @Override
                public ConnectorDeleteTableHandle deserialize(byte[] data)
                {
                    return new TestConnectorDeleteTableHandle(new String(data, UTF_8));
                }
            });
        }

        public Optional<ConnectorCodec<ConnectorMergeTableHandle>> getConnectorMergeTableHandleCodec()
        {
            return Optional.of(new ConnectorCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorMergeTableHandle handle)
                {
                    TestConnectorMergeTableHandle mergeTableHandle = (TestConnectorMergeTableHandle) handle;
                    return mergeTableHandle.getTableName().getBytes(UTF_8);
                }

                @Override
                public ConnectorMergeTableHandle deserialize(byte[] data)
                {
                    return new TestConnectorMergeTableHandle(new String(data, UTF_8));
                }
            });
        }
    }

    /**
     * Test table handle with binary serialization support
     */
    public static class TestConnectorTableHandle
            implements ConnectorTableHandle
    {
        private final String tableName;

        @JsonCreator
        public TestConnectorTableHandle(@JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestConnectorTableHandle that = (TestConnectorTableHandle) obj;
            return Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    /**
     * Test table layout handle with binary serialization support
     */
    public static class TestConnectorTableLayoutHandle
            implements ConnectorTableLayoutHandle
    {
        private final String layoutName;

        public TestConnectorTableLayoutHandle(String layoutName)
        {
            this.layoutName = layoutName;
        }

        public String getLayoutName()
        {
            return layoutName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestConnectorTableLayoutHandle that = (TestConnectorTableLayoutHandle) o;
            return Objects.equals(layoutName, that.layoutName);
        }

        @Override
        public int hashCode()
        {
            return layoutName.hashCode();
        }
    }

    /**
     * Test column handle with binary serialization support
     */
    public static class TestConnectorColumnHandle
            implements ColumnHandle
    {
        private final String columnName;
        private final String columnType;

        @JsonCreator
        public TestConnectorColumnHandle(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("columnType") String columnType)
        {
            this.columnName = columnName;
            this.columnType = columnType;
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public String getColumnType()
        {
            return columnType;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestConnectorColumnHandle that = (TestConnectorColumnHandle) obj;
            return Objects.equals(columnName, that.columnName) &&
                    Objects.equals(columnType, that.columnType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName, columnType);
        }
    }

    /**
     * Test connector handle resolver for codec-enabled connector
     */
    public static class TestConnectorWithCodecHandleResolver
            implements ConnectorHandleResolver
    {
        @Override
        public Class<? extends ConnectorTableHandle> getTableHandleClass()
        {
            return TestConnectorTableHandle.class;
        }

        @Override
        public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
        {
            return TestConnectorTableLayoutHandle.class;
        }

        @Override
        public Class<? extends ColumnHandle> getColumnHandleClass()
        {
            return TestConnectorColumnHandle.class;
        }

        @Override
        public Class<? extends ConnectorSplit> getSplitClass()
        {
            return TestConnectorWithCodecSplit.class;
        }

        @Override
        public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
        {
            return TestConnectorOutputTableHandle.class;
        }

        @Override
        public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
        {
            return TestConnectorInsertTableHandle.class;
        }

        @Override
        public Class<? extends ConnectorDeleteTableHandle> getDeleteTableHandleClass()
        {
            return TestConnectorDeleteTableHandle.class;
        }
    }

    /**
     * Test output table handle with binary serialization support
     */
    public static class TestConnectorOutputTableHandle
            implements ConnectorOutputTableHandle
    {
        private final String tableName;

        @JsonCreator
        public TestConnectorOutputTableHandle(
                @JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestConnectorOutputTableHandle that = (TestConnectorOutputTableHandle) o;
            return tableName.equals(that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    /**
     * Test insert table handle with binary serialization support
     */
    public static class TestConnectorInsertTableHandle
            implements ConnectorInsertTableHandle
    {
        private final String tableName;

        @JsonCreator
        public TestConnectorInsertTableHandle(
                @JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestConnectorInsertTableHandle that = (TestConnectorInsertTableHandle) o;
            return tableName.equals(that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    /**
     * Test delete table handle with binary serialization support
     */
    public static class TestConnectorDeleteTableHandle
            implements ConnectorDeleteTableHandle
    {
        private final String tableName;

        @JsonCreator
        public TestConnectorDeleteTableHandle(
                @JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestConnectorDeleteTableHandle that = (TestConnectorDeleteTableHandle) o;
            return tableName.equals(that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    /**
     * Test merge table handle with binary serialization support
     */
    public static class TestConnectorMergeTableHandle
            implements ConnectorMergeTableHandle
    {
        private final String tableName;

        @JsonCreator
        public TestConnectorMergeTableHandle(
                @JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @Override
        public ConnectorTableHandle getTableHandle()
        {
            throw new UnsupportedOperationException("Merge table handles not supported");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestConnectorMergeTableHandle that = (TestConnectorMergeTableHandle) o;
            return tableName.equals(that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    public static class TestConnectorWithoutCodecSplit
            implements ConnectorSplit
    {
        private final String data;
        private final int sequence;

        @JsonCreator
        public TestConnectorWithoutCodecSplit(
                @JsonProperty("data") String data,
                @JsonProperty("sequence") int sequence)
        {
            this.data = data;
            this.sequence = sequence;
        }

        @JsonProperty
        public String getData()
        {
            return data;
        }

        @JsonProperty
        public int getSequence()
        {
            return sequence;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NodeSelectionStrategy.NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return ImmutableMap.of("data", data, "sequence", sequence);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestConnectorWithoutCodecSplit that = (TestConnectorWithoutCodecSplit) obj;
            return sequence == that.sequence && Objects.equals(data, that.data);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(data, sequence);
        }
    }

    public static class TestConnectorWithoutCodecHandleResolver
            implements ConnectorHandleResolver
    {
        @Override
        public Class<? extends ConnectorTableHandle> getTableHandleClass()
        {
            throw new UnsupportedOperationException("Table handles not supported");
        }

        @Override
        public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
        {
            throw new UnsupportedOperationException("Table layout handles not supported");
        }

        @Override
        public Class<? extends ColumnHandle> getColumnHandleClass()
        {
            throw new UnsupportedOperationException("Column handles not supported");
        }

        @Override
        public Class<? extends ConnectorSplit> getSplitClass()
        {
            return TestConnectorWithoutCodecSplit.class;
        }

        @Override
        public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
        {
            throw new UnsupportedOperationException("Output table handles not supported");
        }

        @Override
        public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
        {
            throw new UnsupportedOperationException("Insert table handles not supported");
        }

        @Override
        public Class<? extends ConnectorDeleteTableHandle> getDeleteTableHandleClass()
        {
            throw new UnsupportedOperationException("Delete table handles not supported");
        }

        @Override
        public Class<? extends ConnectorMergeTableHandle> getMergeTableHandleClass()
        {
            throw new UnsupportedOperationException("Merge table handles not supported");
        }
    }
}
