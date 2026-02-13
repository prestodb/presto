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
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeTaskRdd;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskRdd;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link PrestoSparkRddFactory}.
 *
 * These tests verify that createSparkRdd correctly creates PrestoSparkNativeTaskRdd
 * when native execution is enabled via session property, and PrestoSparkTaskRdd
 * when native execution is disabled.
 */
@Test(singleThreaded = true)
public class TestPrestoSparkRddFactory
{
    @Mock
    private JavaSparkContext mockSparkContext;

    @Mock
    private SparkContext mockSparkContextSc;

    @Mock
    private PlanFragment mockFragment;

    @Mock
    private PrestoSparkTaskExecutorFactoryProvider mockExecutorFactoryProvider;

    @Mock
    private TableWriteInfo mockTableWriteInfo;

    @Mock
    private TempStorage mockTempStorage;

    @Mock
    private TempStorageHandle mockTempStorageHandle;

    @Mock
    private StageExecutionDescriptor mockStageExecutionDescriptor;

    @Mock
    private CollectionAccumulator<SerializedTaskInfo> mockTaskInfoCollector;

    @Mock
    private CollectionAccumulator<PrestoSparkShuffleStats> mockShuffleStatsCollector;

    private AutoCloseable mockitoCloseable;
    private PrestoSparkRddFactory rddFactory;
    private SplitManager splitManager;

    @BeforeMethod
    public void setUp()
    {
        mockitoCloseable = MockitoAnnotations.openMocks(this);

        // Set up mock behaviors
        when(mockSparkContext.sc()).thenReturn(mockSparkContextSc);
        when(mockTempStorage.getRootDirectoryHandle()).thenReturn(mockTempStorageHandle);
        when(mockTempStorage.serializeHandle(any())).thenReturn("serialized-handle".getBytes());
        when(mockStageExecutionDescriptor.isStageGroupedExecution()).thenReturn(false);
        when(mockFragment.getStageExecutionDescriptor()).thenReturn(mockStageExecutionDescriptor);
        when(mockFragment.getId()).thenReturn(new PlanFragmentId(1));
        when(mockFragment.getRemoteSourceNodes()).thenReturn(ImmutableList.of());

        // Create a ValuesNode as the root (no table scans)
        ValuesNode valuesNode = new ValuesNode(
                Optional.empty(),
                new PlanNodeId("values"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
        when(mockFragment.getRoot()).thenReturn(valuesNode);

        // Create the factory with mocked dependencies
        splitManager = mock(SplitManager.class);
        PartitioningProviderManager partitioningProviderManager = new PartitioningProviderManager();
        JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec = JsonCodec.jsonCodec(PrestoSparkTaskDescriptor.class);
        Codec<TaskSource> taskSourceCodec = createTestTaskSourceCodec();
        FeaturesConfig featuresConfig = new FeaturesConfig();

        rddFactory = new PrestoSparkRddFactory(
                splitManager,
                partitioningProviderManager,
                taskDescriptorJsonCodec,
                taskSourceCodec,
                featuresConfig);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        if (mockitoCloseable != null) {
            mockitoCloseable.close();
        }
    }

    /**
     * Test that createSparkRdd creates PrestoSparkNativeTaskRdd when native execution
     * is enabled via session property.
     */
    @Test
    public void testCreateSparkRddWithNativeExecutionEnabled()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();

        when(mockFragment.getPartitioning()).thenReturn(SINGLE_DISTRIBUTION);

        Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.of();
        Map<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.of();

        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> result = rddFactory.createSparkRdd(
                mockSparkContext,
                session,
                mockFragment,
                rddInputs,
                broadcastInputs,
                mockExecutorFactoryProvider,
                mockTaskInfoCollector,
                mockShuffleStatsCollector,
                mockTableWriteInfo,
                PrestoSparkSerializedPage.class,
                mockTempStorage);

        assertNotNull(result, "RDD should not be null");
        // The underlying RDD should be PrestoSparkNativeTaskRdd when native execution is enabled
        assertTrue(result.rdd() instanceof PrestoSparkNativeTaskRdd,
                "RDD should be PrestoSparkNativeTaskRdd when native execution is enabled, but was: " + result.rdd().getClass().getName());
    }

    /**
     * Test that createSparkRdd creates PrestoSparkTaskRdd when native execution
     * is disabled via session property.
     */
    @Test
    public void testCreateSparkRddWithNativeExecutionDisabled()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "false")
                .build();

        when(mockFragment.getPartitioning()).thenReturn(SINGLE_DISTRIBUTION);

        Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.of();
        Map<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.of();

        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> result = rddFactory.createSparkRdd(
                mockSparkContext,
                session,
                mockFragment,
                rddInputs,
                broadcastInputs,
                mockExecutorFactoryProvider,
                mockTaskInfoCollector,
                mockShuffleStatsCollector,
                mockTableWriteInfo,
                PrestoSparkSerializedPage.class,
                mockTempStorage);

        assertNotNull(result, "RDD should not be null");
        // The underlying RDD should be PrestoSparkTaskRdd when native execution is disabled
        assertTrue(result.rdd() instanceof PrestoSparkTaskRdd,
                "RDD should be PrestoSparkTaskRdd when native execution is disabled, but was: " + result.rdd().getClass().getName());
    }

    /**
     * Test that different sessions produce different RDD types based on their
     * native execution settings. This validates the per-session control.
     */
    @Test
    public void testDifferentSessionsProduceDifferentRddTypes()
    {
        when(mockFragment.getPartitioning()).thenReturn(SINGLE_DISTRIBUTION);

        Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.of();
        Map<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.of();

        // Session with native execution enabled
        Session nativeEnabledSession = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();

        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> nativeResult = rddFactory.createSparkRdd(
                mockSparkContext,
                nativeEnabledSession,
                mockFragment,
                rddInputs,
                broadcastInputs,
                mockExecutorFactoryProvider,
                mockTaskInfoCollector,
                mockShuffleStatsCollector,
                mockTableWriteInfo,
                PrestoSparkSerializedPage.class,
                mockTempStorage);

        // Session with native execution disabled
        Session nativeDisabledSession = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "false")
                .build();

        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> javaResult = rddFactory.createSparkRdd(
                mockSparkContext,
                nativeDisabledSession,
                mockFragment,
                rddInputs,
                broadcastInputs,
                mockExecutorFactoryProvider,
                mockTaskInfoCollector,
                mockShuffleStatsCollector,
                mockTableWriteInfo,
                PrestoSparkSerializedPage.class,
                mockTempStorage);

        // Verify different RDD types based on session settings
        assertTrue(nativeResult.rdd() instanceof PrestoSparkNativeTaskRdd,
                "Native enabled session should produce PrestoSparkNativeTaskRdd");
        assertTrue(javaResult.rdd() instanceof PrestoSparkTaskRdd,
                "Native disabled session should produce PrestoSparkTaskRdd");
    }

    /**
     * Test that createSparkRdd throws PrestoException for SCALED_WRITER_DISTRIBUTION.
     */
    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Automatic writers scaling is not supported.*")
    public void testCreateSparkRddRejectsScaledWriterDistribution()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "false")
                .build();

        when(mockFragment.getPartitioning()).thenReturn(SCALED_WRITER_DISTRIBUTION);

        Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.of();
        Map<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.of();

        rddFactory.createSparkRdd(
                mockSparkContext,
                session,
                mockFragment,
                rddInputs,
                broadcastInputs,
                mockExecutorFactoryProvider,
                mockTaskInfoCollector,
                mockShuffleStatsCollector,
                mockTableWriteInfo,
                PrestoSparkSerializedPage.class,
                mockTempStorage);
    }

    private Codec<TaskSource> createTestTaskSourceCodec()
    {
        return new Codec<TaskSource>()
        {
            @Override
            public byte[] toBytes(TaskSource value)
            {
                return new byte[0];
            }

            @Override
            public TaskSource fromBytes(byte[] bytes)
            {
                return null;
            }

            @Override
            public TaskSource readBytes(java.io.InputStream input)
            {
                return null;
            }

            @Override
            public void writeBytes(java.io.OutputStream output, TaskSource value)
            {
            }
        };
    }
}
