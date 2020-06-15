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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.execution.PrestoSparkRemoteSourceOperator.SparkRemoteSourceOperatorFactory;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static com.facebook.presto.spark.util.PrestoSparkUtils.transformRowsToPages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.transform;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceFactory
        implements RemoteSourceFactory
{
    private final PagesSerde pagesSerde;
    private final Map<PlanNodeId, Iterator<PrestoSparkMutableRow>> shuffleInputs;
    private final Map<PlanNodeId, Iterator<PrestoSparkSerializedPage>> broadcastInputs;

    public PrestoSparkRemoteSourceFactory(
            PagesSerde pagesSerde,
            Map<PlanNodeId, Iterator<PrestoSparkMutableRow>> shuffleInputs,
            Map<PlanNodeId, Iterator<PrestoSparkSerializedPage>> broadcastInputs)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.shuffleInputs = ImmutableMap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        this.broadcastInputs = ImmutableMap.copyOf(requireNonNull(broadcastInputs, "broadcastInputs is null"));
    }

    @Override
    public OperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        Iterator<PrestoSparkMutableRow> shuffleInput = shuffleInputs.get(planNodeId);
        Iterator<PrestoSparkSerializedPage> broadcastInput = broadcastInputs.get(planNodeId);
        checkArgument(shuffleInput != null || broadcastInput != null, "input not found for plan node with id %s", planNodeId);
        checkArgument(shuffleInput == null || broadcastInput == null, "single remote source cannot accept both, broadcast and shuffle inputs");

        if (broadcastInput != null) {
            return new SparkRemoteSourceOperatorFactory(
                    operatorId,
                    planNodeId,
                    transform(broadcastInput, sparkSerializedPage -> pagesSerde.deserialize(toSerializedPage(sparkSerializedPage))));
        }

        return new SparkRemoteSourceOperatorFactory(
                operatorId,
                planNodeId,
                transformRowsToPages(shuffleInput, types));
    }

    @Override
    public OperatorFactory createMergeRemoteSource(
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
