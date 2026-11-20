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
package com.facebook.presto.spark.planner;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.CollectionAccumulator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.List$;
import scala.reflect.ClassTag;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spark.util.PrestoSparkUtils.classTag;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkRddFactory
{
    @DataProvider
    public Object[][] countedHashPartitions()
    {
        return new Object[][] {{1}, {2}, {8}, {64}};
    }

    @Test(dataProvider = "countedHashPartitions")
    @SuppressWarnings("unchecked")
    public void testConstructsRddForCountedHashFragment(int partitionCount)
    {
        SparkContext context = mock(SparkContext.class);
        when(context.getConf()).thenReturn(new SparkConf(false));
        AtomicInteger rddIds = new AtomicInteger();
        when(context.newRddId()).thenAnswer(invocation -> rddIds.incrementAndGet());
        when(context.clean(any(), eq(true))).thenAnswer(invocation -> invocation.getArguments()[0]);
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(context);
        PartitioningHandle partitioning = createSystemPartitioning(FIXED, HASH, partitionCount);
        PlanFragmentId inputFragmentId = new PlanFragmentId(0);
        PlanFragmentId outputFragmentId = new PlanFragmentId(1);
        VariableReferenceExpression input = new VariableReferenceExpression(Optional.empty(), "input", BIGINT);
        RemoteSourceNode remoteSource = new RemoteSourceNode(Optional.empty(), new PlanNodeId("source"), inputFragmentId,
                ImmutableList.of(input), false, Optional.empty(), ExchangeNode.Type.REPARTITION);
        PlanFragment fragment = mock(PlanFragment.class);
        when(fragment.getId()).thenReturn(outputFragmentId);
        when(fragment.getPartitioning()).thenReturn(partitioning);
        when(fragment.getRoot()).thenReturn(remoteSource);
        when(fragment.getRemoteSourceNodes()).thenReturn(ImmutableList.of(remoteSource));
        when(fragment.getStageExecutionDescriptor()).thenReturn(StageExecutionDescriptor.ungroupedExecution());
        JsonCodec<PrestoSparkTaskDescriptor> descriptorCodec = mock(JsonCodec.class);
        when(descriptorCodec.toJsonBytes(any())).thenReturn(new byte[0]);
        TempStorage storage = mock(TempStorage.class);
        when(storage.getRootDirectoryHandle()).thenReturn(mock(TempStorageHandle.class));
        when(storage.serializeHandle(any())).thenReturn(new byte[0]);
        Codec<TaskSource> taskSourceCodec = mock(Codec.class);
        PrestoSparkRddFactory factory = new PrestoSparkRddFactory(mock(SplitManager.class), mock(Metadata.class),
                new PartitioningProviderManager(), descriptorCodec, taskSourceCodec, new FeaturesConfig());
        Session session = testSessionBuilder().setSystemProperty("native_execution_enabled", "true").build();
        JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> inputRdd = JavaPairRDD.fromRDD(
                new EmptyInputRdd(context, partitionCount), classTag(MutablePartitionId.class), classTag(PrestoSparkMutableRow.class));
        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> outputRdd = factory.createSparkRdd(
                sparkContext, session, fragment, ImmutableMap.of(inputFragmentId, inputRdd), ImmutableMap.of(),
                mock(PrestoSparkTaskExecutorFactoryProvider.class), new CollectionAccumulator<SerializedTaskInfo>(),
                new CollectionAccumulator<PrestoSparkShuffleStats>(), new TableWriteInfo(Optional.empty(), Optional.empty()),
                PrestoSparkSerializedPage.class, storage);
        assertEquals(outputRdd.getNumPartitions(), partitionCount);
    }

    private static class EmptyInputRdd
            extends RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>
    {
        private final int partitionCount;

        private EmptyInputRdd(SparkContext context, int partitionCount)
        {
            super(context, List$.MODULE$.empty(), tupleClassTag());
            this.partitionCount = partitionCount;
        }

        @SuppressWarnings("unchecked")
        private static ClassTag<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> tupleClassTag()
        {
            return (ClassTag<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>) (ClassTag<?>) classTag(Tuple2.class);
        }

        @Override
        public Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> compute(Partition partition, TaskContext context)
        {
            throw new AssertionError("RDD construction must not execute tasks");
        }

        @Override
        public Partition[] getPartitions()
        {
            Partition[] partitions = new Partition[partitionCount];
            for (int index = 0; index < partitionCount; index++) {
                int partitionIndex = index;
                partitions[index] = new Partition()
                {
                    @Override
                    public int index()
                    {
                        return partitionIndex;
                    }
                };
            }
            return partitions;
        }
    }
}
