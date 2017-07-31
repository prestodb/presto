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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DynamicFilterSourceOperator
        implements Operator
{
    public static class DynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final List<Integer> filterChannels;
        private final List<String> filterDynamicFilterNames;
        private final DynamicFilterClientSupplier dynamicFilterClientSupplier;
        private final String sourceId;
        private final int expectedDrivers;

        private boolean closed;
        private int driverId;

        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> filterChannels,
                List<String> filterDynamicFilterNames,
                DynamicFilterClientSupplier dynamicFilterClientSupplier,
                String sourceId,
                int expectedDrivers)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(types);
            this.filterChannels = ImmutableList.copyOf(filterChannels);
            this.filterDynamicFilterNames = ImmutableList.copyOf(filterDynamicFilterNames);
            this.dynamicFilterClientSupplier = requireNonNull(dynamicFilterClientSupplier, "dynamicFilterClientSupplier is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.expectedDrivers = expectedDrivers;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterSourceOperator.class.getSimpleName());

            return new DynamicFilterSourceOperator(operatorContext,
                    types,
                    filterChannels,
                    filterDynamicFilterNames,
                    dynamicFilterClientSupplier.createClient(driverContext.getPipelineContext().getTaskId(), sourceId, driverId++, expectedDrivers));
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel dynamic filter build can not be duplicated");
        }
    }

    private static final int DEFAULT_POSITIONS_LIMIT = 1000;

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final List<Integer> filterChannels;
    private final List<String> filterDynamicFilterNames;
    private final DynamicFilterClient client;
    private final int positionsLimit;

    private Page page;
    private boolean finishing;
    private long positionCount;
    private List<ImmutableList.Builder<Object>> valuesBuilder;
    private boolean[] nullsAllowed;

    public DynamicFilterSourceOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> filterChannels,
            List<String> filterDynamicFilterNames,
            DynamicFilterClient client)
    {
        this(operatorContext, types, filterChannels, filterDynamicFilterNames, client, DEFAULT_POSITIONS_LIMIT);
    }

    public DynamicFilterSourceOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> filterChannels,
            List<String> filterDynamicFilterNames,
            DynamicFilterClient client,
            int positionsLimit)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(types);
        this.filterChannels = ImmutableList.copyOf(filterChannels);
        this.filterDynamicFilterNames = ImmutableList.copyOf(filterDynamicFilterNames);
        this.valuesBuilder = filterChannels.stream().map(channel -> ImmutableList.builder()).collect(toImmutableList());
        this.client = requireNonNull(client, "client is null");
        this.nullsAllowed = new boolean[filterChannels.size()];

        checkArgument(positionsLimit > 0, "positionsLimit must be greater than zero");
        this.positionsLimit = positionsLimit;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;
        sendTupleDomain();
    }

    @Override
    public boolean isFinished()
    {
        return finishing && page == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        this.page = page;
        processPage();
    }

    @Override
    public Page getOutput()
    {
        Page page = this.page;
        this.page = null;
        return page;
    }

    private void processPage()
    {
        positionCount += page.getPositionCount();

        // we don't want to filter if there're too many values
        if (positionCount > positionsLimit) {
            return;
        }

        for (int channelIndex = 0; channelIndex < filterChannels.size(); channelIndex++) {
            Integer channel = filterChannels.get(channelIndex);
            for (int position = 0; position < page.getBlock(channel).getPositionCount(); position++) {
                Object value = TypeUtils.readNativeValue(types.get(channel), page.getBlock(channel), position);
                if (value == null) {
                    nullsAllowed[channelIndex] = true;
                }
                else {
                    valuesBuilder.get(channelIndex).add(value);
                }
            }
        }
    }

    private void sendTupleDomain()
    {
        // we don't want to filter if there're too many values
        if (positionCount > positionsLimit) {
            client.storeSummary(new DynamicFilterSummary(TupleDomain.all()));
            return;
        }

        ImmutableMap.Builder<String, Domain> domainsBuilder = ImmutableMap.builder();
        for (int channelIndex = 0; channelIndex < filterChannels.size(); channelIndex++) {
            Integer channel = filterChannels.get(channelIndex);
            List<Object> values = valuesBuilder.get(channelIndex).build();
            Domain domain;
            if (values.isEmpty()) {
                domain = Domain.none(types.get(channel));
            }
            else {
                domain = Domain.create(ValueSet.copyOf(types.get(channel), values), nullsAllowed[channelIndex]);
            }
            domainsBuilder.put(filterDynamicFilterNames.get(channelIndex).toString(), domain);
        }

        client.storeSummary(new DynamicFilterSummary(TupleDomain.withColumnDomains(domainsBuilder.build())));
    }
}
