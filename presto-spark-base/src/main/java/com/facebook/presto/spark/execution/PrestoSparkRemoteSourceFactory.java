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
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.execution.PrestoSparkRemoteSourceOperator.SparkRemoteSourceOperatorFactory;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceFactory
        implements RemoteSourceFactory
{
    private final PagesSerde pagesSerde;
    private final Map<PlanNodeId, List<scala.collection.Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>>> rowInputsMap;
    private final Map<PlanNodeId, List<java.util.Iterator<PrestoSparkSerializedPage>>> pageInputsMap;

    public PrestoSparkRemoteSourceFactory(
            PagesSerde pagesSerde,
            Map<PlanNodeId, List<scala.collection.Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>>> rowInputsMap,
            Map<PlanNodeId, List<java.util.Iterator<PrestoSparkSerializedPage>>> pageInputsMap)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.rowInputsMap = requireNonNull(rowInputsMap, "rowInputs is null");
        this.pageInputsMap = requireNonNull(pageInputsMap, "pageInputs is null");
    }

    @Override
    public OperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        List<scala.collection.Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> rowInputs = rowInputsMap.get(planNodeId);
        List<java.util.Iterator<PrestoSparkSerializedPage>> pageInputs = pageInputsMap.get(planNodeId);
        checkArgument(rowInputs != null || pageInputs != null, "input not found for plan node with id %s", planNodeId);
        checkArgument(rowInputs == null || pageInputs == null, "single remote source cannot accept both, row and page inputs");

        if (pageInputs != null) {
            return new SparkRemoteSourceOperatorFactory(
                    operatorId,
                    planNodeId,
                    new PrestoSparkSerializedPageInput(pagesSerde, pageInputs));
        }

        return new SparkRemoteSourceOperatorFactory(
                operatorId,
                planNodeId,
                new PrestoSparkMutableRowPageInput(types, rowInputs));
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
