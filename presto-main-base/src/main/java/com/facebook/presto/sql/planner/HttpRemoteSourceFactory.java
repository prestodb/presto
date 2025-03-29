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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.operator.MergeOperator.MergeOperatorFactory;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TaskExchangeClientManager;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.OrderingCompiler;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isExchangeChecksumEnabled;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static java.util.Objects.requireNonNull;

public class HttpRemoteSourceFactory
        implements RemoteSourceFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final TaskExchangeClientManager taskExchangeClientManager;
    private final OrderingCompiler orderingCompiler;

    public HttpRemoteSourceFactory(BlockEncodingSerde blockEncodingSerde, TaskExchangeClientManager taskExchangeClientManager, OrderingCompiler orderingCompiler)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.taskExchangeClientManager = requireNonNull(taskExchangeClientManager, "taskExchangeClientManager is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
    }

    @Override
    public SourceOperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        return new ExchangeOperatorFactory(
                operatorId,
                planNodeId,
                taskExchangeClientManager,
                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session), isExchangeChecksumEnabled(session)));
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
        return new MergeOperatorFactory(
                operatorId,
                planNodeId,
                taskExchangeClientManager,
                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session), isExchangeChecksumEnabled(session)),
                orderingCompiler,
                types,
                outputChannels,
                sortChannels,
                sortOrder);
    }
}
