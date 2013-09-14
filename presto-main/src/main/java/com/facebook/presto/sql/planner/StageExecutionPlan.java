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

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StageExecutionPlan
{
    private final PlanFragment fragment;
    private final Optional<DataSource> dataSource;
    private final List<StageExecutionPlan> subStages;
    private final List<TupleInfo> tupleInfos;
    private final Optional<List<String>> fieldNames;
    private final Map<PlanNodeId, OutputReceiver> outputReceivers;

    public StageExecutionPlan(PlanFragment fragment, Optional<DataSource> dataSource, List<StageExecutionPlan> subStages, Map<PlanNodeId, OutputReceiver> outputReceivers)
    {
        this.fragment = checkNotNull(fragment, "fragment is null");
        this.dataSource = checkNotNull(dataSource, "dataSource is null");
        this.subStages = ImmutableList.copyOf(checkNotNull(subStages, "dependencies is null"));
        this.outputReceivers = ImmutableMap.copyOf(checkNotNull(outputReceivers, "outputReceivers is null"));

        tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
                .transform(Functions.forMap(fragment.getSymbols()))
                .transform(com.facebook.presto.sql.analyzer.Type.toRaw())
                .transform(new Function<Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list());
        fieldNames = (fragment.getRoot() instanceof OutputNode) ?
                Optional.<List<String>>of(ImmutableList.copyOf(((OutputNode) fragment.getRoot()).getColumnNames())) :
                Optional.<List<String>>absent();
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public List<String> getFieldNames()
    {
        checkState(fieldNames.isPresent(), "cannot get field names from non-output stage");
        return fieldNames.get();
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public Optional<DataSource> getDataSource()
    {
        return dataSource;
    }

    public List<StageExecutionPlan> getSubStages()
    {
        return subStages;
    }

    public Map<PlanNodeId, OutputReceiver> getOutputReceivers()
    {
        return outputReceivers;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("fragment", fragment)
                .add("dataSource", dataSource)
                .add("subStages", subStages)
                .add("outputReceivers", outputReceivers)
                .toString();
    }
}
