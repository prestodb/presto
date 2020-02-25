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
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spark.execution.PrestoSparkRemoteSourceOperator.SparkRemoteSourceOperatorFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceFactory
        implements RemoteSourceFactory
{
    private final Map<PlanNodeId, Iterator<SerializedPage>> inputs;
    private final BlockEncodingSerde blockEncodingSerde;

    public PrestoSparkRemoteSourceFactory(Map<PlanNodeId, Iterator<SerializedPage>> inputs, BlockEncodingSerde blockEncodingSerde)
    {
        this.inputs = ImmutableMap.copyOf(requireNonNull(inputs, "inputs is null"));
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Override
    public OperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        return new SparkRemoteSourceOperatorFactory(
                operatorId,
                planNodeId,
                requireNonNull(inputs.get(planNodeId), format("input is missing for plan node: %s", planNodeId)),
                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)).createPagesSerde());
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
