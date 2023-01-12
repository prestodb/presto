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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleInfoTranslator;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBatchTaskUpdateRequest
{
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<PrestoSparkLocalShuffleReadInfo> PRESTO_SPARK_LOCAL_SHUFFLE_READ_INFO_JSON_CODEC = JsonCodec.jsonCodec(PrestoSparkLocalShuffleReadInfo.class);
    private static final JsonCodec<PrestoSparkLocalShuffleWriteInfo> PRESTO_SPARK_LOCAL_SHUFFLE_WRITE_INFO_JSON_CODEC = JsonCodec.jsonCodec(PrestoSparkLocalShuffleWriteInfo.class);

    @Test
    public void testJsonConversion()
            throws Exception
    {
        PrestoSparkLocalShuffleInfoTranslator shuffleInfoTranslator = new PrestoSparkLocalShuffleInfoTranslator(
                PRESTO_SPARK_LOCAL_SHUFFLE_READ_INFO_JSON_CODEC,
                PRESTO_SPARK_LOCAL_SHUFFLE_WRITE_INFO_JSON_CODEC);
        PrestoSparkLocalShuffleReadInfo readInfo = new PrestoSparkLocalShuffleReadInfo(0, "/dummy/read/path");

        String stringSerializedReadInfo = shuffleInfoTranslator.createSerializedReadInfo(readInfo);
        PlanNodeId planNodeId = new PlanNodeId("planNodeId");
        List<TaskSource> sources = new ArrayList<>();
        sources.add(
                new TaskSource(
                        planNodeId,
                        ImmutableSet.of(
                                new ScheduledSplit(
                                        0,
                                        planNodeId,
                                        new Split(
                                                new ConnectorId("connector_id"),
                                                new RemoteTransactionHandle(),
                                                new RemoteSplit(new Location(stringSerializedReadInfo), TaskId.valueOf("foo.0.0.0"))))),
                        true));
        Session session = TestingSession.testSessionBuilder().build();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                Optional.of(createPlanFragment().toBytes(PLAN_FRAGMENT_JSON_CODEC)),
                sources,
                createInitialEmptyOutputBuffers(PARTITIONED),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));
        String shuffleWriteInfo = "dummy-shuffle-write-info";
        BatchTaskUpdateRequest batchUpdateRequest = new BatchTaskUpdateRequest(updateRequest, Optional.of(shuffleWriteInfo.getBytes()));
        JsonCodec<BatchTaskUpdateRequest> batchTaskUpdateRequestJsonCodec = getJsonCodec();
        byte[] batchUpdateRequestJson = batchTaskUpdateRequestJsonCodec.toBytes(batchUpdateRequest);
        BatchTaskUpdateRequest recoveredBatchUpdateRequest = batchTaskUpdateRequestJsonCodec.fromBytes(batchUpdateRequestJson);
        List<TaskSource> recoveredSources = recoveredBatchUpdateRequest.getTaskUpdateRequest().getSources();
        assertEquals(1, recoveredSources.size());
        TaskSource source = recoveredSources.get(0);
        assertEquals(planNodeId, source.getPlanNodeId());
        assertEquals(1, source.getSplits().size());
        assertTrue(((ScheduledSplit) source.getSplits().toArray()[0]).getSplit().getConnectorSplit() instanceof RemoteSplit);
        RemoteSplit remoteSplit = (RemoteSplit) ((ScheduledSplit) source.getSplits().toArray()[0]).getSplit().getConnectorSplit();
        assertEquals(stringSerializedReadInfo, remoteSplit.getLocation().getLocation());
        assertEquals(batchUpdateRequest.getShuffleWriteInfo().get(), recoveredBatchUpdateRequest.getShuffleWriteInfo().get());
        assertEquals(batchUpdateRequest.getTaskUpdateRequest().getExtraCredentials(), recoveredBatchUpdateRequest.getTaskUpdateRequest().getExtraCredentials());
        assertEquals(batchUpdateRequest.getTaskUpdateRequest().getSession().getCatalog(), recoveredBatchUpdateRequest.getTaskUpdateRequest().getSession().getCatalog());
    }

    @Test
    public void testShuffleInfoSerialization()
    {
        PrestoSparkLocalShuffleInfoTranslator shuffleTranslator = new PrestoSparkLocalShuffleInfoTranslator(
                PRESTO_SPARK_LOCAL_SHUFFLE_READ_INFO_JSON_CODEC,
                PRESTO_SPARK_LOCAL_SHUFFLE_WRITE_INFO_JSON_CODEC);
        PrestoSparkLocalShuffleReadInfo readInfo = new PrestoSparkLocalShuffleReadInfo(0, "/dummy/read/path");
        PrestoSparkLocalShuffleWriteInfo writeInfo = new PrestoSparkLocalShuffleWriteInfo(1, "/dummy/write/path");
        String stringSerializedReadInfo = shuffleTranslator.createSerializedReadInfo(readInfo);
        String stringSerializedWriteInfo = shuffleTranslator.createSerializedWriteInfo(writeInfo);
        assertEquals(
                stringSerializedReadInfo,
                "{\n" +
                        "  \"numPartitions\" : 0,\n" +
                        "  \"rootPath\" : \"/dummy/read/path\"\n" +
                        "}");
        assertEquals(
                stringSerializedWriteInfo,
                "{\n" +
                        "  \"numPartitions\" : 1,\n" +
                        "  \"rootPath\" : \"/dummy/write/path\"\n" +
                        "}");
    }

    private JsonCodec<BatchTaskUpdateRequest> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
            jsonCodecBinder(binder).bindJsonCodec(BatchTaskUpdateRequest.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<BatchTaskUpdateRequest>>() {});
    }
}
