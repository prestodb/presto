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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkStorageHandle;
import com.facebook.presto.spark.execution.PrestoSparkRemoteSourceOperator.SparkRemoteSourceOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.util.CollectionAccumulator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.isStorageBasedBroadcastJoinEnabled;
import static com.facebook.presto.spark.util.PrestoSparkUtils.createPagesSerde;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceFactory
        implements RemoteSourceFactory
{
    private final BlockEncodingManager blockEncodingManager;
    private final Map<PlanNodeId, List<PrestoSparkShuffleInput>> shuffleInputsMap;
    private final Map<PlanNodeId, List<java.util.Iterator<PrestoSparkSerializedPage>>> pageInputsMap;
    private final Map<PlanNodeId, List<?>> broadcastInputsMap;
    private final int taskId;
    private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
    private final TempStorage tempStorage;
    private final TempDataOperationContext tempDataOperationContext;
    private final PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager;
    private final StageId stageId;

    public PrestoSparkRemoteSourceFactory(
            BlockEncodingManager blockEncodingManager,
            Map<PlanNodeId, List<PrestoSparkShuffleInput>> shuffleInputsMap,
            Map<PlanNodeId, List<Iterator<PrestoSparkSerializedPage>>> pageInputsMap,
            Map<PlanNodeId, List<?>> broadcastInputsMap,
            int taskId,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            TempStorage tempStorage,
            TempDataOperationContext tempDataOperationContext,
            PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager,
            StageId stageId)
    {
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.shuffleInputsMap = ImmutableMap.copyOf(requireNonNull(shuffleInputsMap, "shuffleInputsMap is null"));
        this.pageInputsMap = ImmutableMap.copyOf(requireNonNull(pageInputsMap, "pageInputs is null"));
        this.broadcastInputsMap = ImmutableMap.copyOf(requireNonNull(broadcastInputsMap, "broadcastInputsMap is null"));
        this.taskId = taskId;
        this.shuffleStatsCollector = requireNonNull(shuffleStatsCollector, "shuffleStatsCollector is null");
        this.tempDataOperationContext = requireNonNull(tempDataOperationContext, "tempDataOperationContext is null");
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.prestoSparkBroadcastTableCacheManager = requireNonNull(prestoSparkBroadcastTableCacheManager, "prestoSparkBroadcastTableCacheManager is null");
        this.stageId = requireNonNull(stageId, "stageId is null");
    }

    @Override
    public SourceOperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        List<PrestoSparkShuffleInput> shuffleInputs = shuffleInputsMap.get(planNodeId);
        List<java.util.Iterator<PrestoSparkSerializedPage>> pageInputs = pageInputsMap.get(planNodeId);
        List<?> broadcastInputs = broadcastInputsMap.get(planNodeId);
        checkArgument(shuffleInputs != null || pageInputs != null || broadcastInputs != null, "input not found for plan node with id %s", planNodeId);
        checkArgument(shuffleInputs == null || pageInputs == null, "single remote source cannot accept both, shuffle and page inputs");
        if (broadcastInputs != null) {
            if (isStorageBasedBroadcastJoinEnabled(session)) {
                List<List<PrestoSparkStorageHandle>> diskPageInputs =
                        broadcastInputs.stream()
                                .map(input -> ((List<?>) input).stream().map(PrestoSparkStorageHandle.class::cast).collect(toImmutableList()))
                                .collect(toImmutableList());
                return new SparkRemoteSourceOperatorFactory(
                        operatorId,
                        planNodeId,
                        new PrestoSparkDiskPageInput(
                                createPagesSerde(blockEncodingManager),
                                tempStorage,
                                tempDataOperationContext,
                                prestoSparkBroadcastTableCacheManager,
                                stageId,
                                planNodeId,
                                diskPageInputs));
            }
            else {
                List<Iterator<PrestoSparkSerializedPage>> serializedPageInputs =
                        broadcastInputs.stream()
                                .map(input -> ((List<?>) input).stream().map(PrestoSparkSerializedPage.class::cast).iterator())
                                .collect(toImmutableList());
                return new SparkRemoteSourceOperatorFactory(
                        operatorId,
                        planNodeId,
                        new PrestoSparkSerializedPageInput(createPagesSerde(blockEncodingManager), serializedPageInputs));
            }
        }

        if (pageInputs != null) {
            return new SparkRemoteSourceOperatorFactory(
                    operatorId,
                    planNodeId,
                    new PrestoSparkSerializedPageInput(createPagesSerde(blockEncodingManager), pageInputs));
        }

        return new SparkRemoteSourceOperatorFactory(
                operatorId,
                planNodeId,
                new PrestoSparkShufflePageInput(types, shuffleInputs, taskId, shuffleStatsCollector));
    }

    @Override
    public SourceOperatorFactory createMergeRemoteSource(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            List<Type> types,
            List<Integer> outputChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder)
    {
        throw new UnsupportedOperationException();
    }
}
