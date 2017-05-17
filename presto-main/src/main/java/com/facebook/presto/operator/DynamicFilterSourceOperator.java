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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.predicate.TupleDomain.columnWiseUnion;
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

        private boolean closed;

        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> filterChannels,
                Optional<Integer> hashChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(types);
            this.filterChannels = ImmutableList.<Integer>builder()
                    .addAll(hashChannel.map(ImmutableList::of).orElse(ImmutableList.of()))
                    .addAll(filterChannels)
                    .build();
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

            return new DynamicFilterSourceOperator(operatorContext, types, filterChannels);
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

    @Immutable
    public static class DynamicFilterSummary
            implements Mergeable<DynamicFilterSummary>, OperatorInfo
    {
        private final TupleDomain<Integer> tupleDomain;

        @JsonCreator
        public DynamicFilterSummary(@JsonProperty("tupleDomain") TupleDomain<Integer> tupleDomain)
        {
            this.tupleDomain = tupleDomain;
        }

        @JsonProperty
        public TupleDomain<Integer> getTupleDomain()
        {
            return tupleDomain;
        }

        @Override
        public DynamicFilterSummary mergeWith(DynamicFilterSummary other)
        {
            return new DynamicFilterSummary(columnWiseUnion(tupleDomain, other.tupleDomain));
        }
    }

    private static final int DEFAULT_POSITIONS_LIMIT = 1000;

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final List<Integer> filterChannels;
    private final int positionsLimit;

    private Page page;
    private boolean finishing;
    private long positionCount;
    private List<ImmutableList.Builder<Object>> valuesBuilder;
    private boolean[] nullsAllowed;
    private DynamicFilterSummary summary;

    public DynamicFilterSourceOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> filterChannels)
    {
        this(operatorContext, types, filterChannels, DEFAULT_POSITIONS_LIMIT);
    }

    public DynamicFilterSourceOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> filterChannels,
            int positionsLimit)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(types);
        this.filterChannels = ImmutableList.copyOf(filterChannels);
        this.valuesBuilder = filterChannels.stream().map(channel -> ImmutableList.builder()).collect(toImmutableList());
        this.nullsAllowed = new boolean[filterChannels.size()];

        checkArgument(positionsLimit > 0, "positionsLimit must be greater than zero");
        this.positionsLimit = positionsLimit;

        operatorContext.setInfoSupplier(() -> summary);
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
        buildTupleDomain();
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

    private void buildTupleDomain()
    {
        // we don't want to filter if there're too many values
        if (positionCount > positionsLimit) {
            summary = new DynamicFilterSummary(TupleDomain.all());
            return;
        }

        ImmutableMap.Builder<Integer, Domain> domainsBuilder = ImmutableMap.builder();
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
            domainsBuilder.put(channel, domain);
        }

        summary = new DynamicFilterSummary(TupleDomain.withColumnDomains(domainsBuilder.build()));
    }
}
