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

import com.facebook.presto.Session;
import com.facebook.presto.bloomfilter.BloomFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.facebook.presto.bloomfilter.BloomFilterUtils;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getBloomFilterForDynamicFilteringFalsePositiveProbability;
import static com.facebook.presto.SystemSessionProperties.getBloomFilterForDynamicFilteringSize;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashJoinDynamicFilterSourceOperator
        implements Operator
{
    public static class Channel
    {
        private final Type type;
        private final int index;

        public Channel(Type type, int index)
        {
            this.type = requireNonNull(type, "type is null");
            this.index = index;
        }

        public Type getType()
        {
            return type;
        }

        public int getIndex()
        {
            return index;
        }
    }

    public static class HashJoinDynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final Session session;
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Channel> channels;
        private final DynamicFilterClientSupplier dynamicFilterClientSupplier;
        private final String sourceId;
        private final int expectedDrivers;

        private boolean closed;
        private int driverId;

        public final long defaultBloomFilterSize;
        public final double defaultFpp;
        public final long defaultInsertSize;

        public HashJoinDynamicFilterSourceOperatorFactory(
                Session session,
                int operatorId,
                PlanNodeId planNodeId,
                List<Channel> channels,
                DynamicFilterClientSupplier dynamicFilterClientSupplier,
                String sourceId,
                int expectedDrivers)
        {
            this.session = session;
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.channels = ImmutableList.copyOf(channels);
            this.dynamicFilterClientSupplier = requireNonNull(dynamicFilterClientSupplier, "dynamicFilterClientSupplier is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.expectedDrivers = requireNonNull(expectedDrivers, "expectedDrivers is null");

            this.defaultBloomFilterSize = (long) getBloomFilterForDynamicFilteringSize(session).getValue(DataSize.Unit.BYTE);
            this.defaultFpp = getBloomFilterForDynamicFilteringFalsePositiveProbability(session);
            this.defaultInsertSize = BloomFilterUtils.optimalNumOfItems(defaultBloomFilterSize * 8, defaultFpp);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashJoinDynamicFilterSourceOperator.class.getSimpleName());

            return new HashJoinDynamicFilterSourceOperator(session,
                    operatorContext,
                    channels,
                    dynamicFilterClientSupplier.createClient(driverContext.getPipelineContext().getTaskId(), sourceId, driverId++, expectedDrivers, null),
                    defaultInsertSize,
                    defaultFpp);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash join dynamic filter build can not be duplicated");
        }
    }

    private final Session session;
    private final OperatorContext operatorContext;
    private final List<Channel> channels;
    private final DynamicFilterClient client;
    private final long positionsLimit;
    private final double fpp;

    private Page current;
    private boolean finishing;
    private long positionCount;
    private List<String> values;

    private ImmutableList.Builder[] builders;

    public HashJoinDynamicFilterSourceOperator(
            Session session,
            OperatorContext operatorContext,
            List<Channel> channels,
            DynamicFilterClient dynamicFilterClient,
            long positionsLimit,
            double fpp)
    {
        this.session = session;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.channels = ImmutableList.copyOf(channels);
        this.values = new ArrayList<>();
        this.client = requireNonNull(dynamicFilterClient, "client is null");

        checkArgument(positionsLimit > 0, "positionsLimit must be greater than zero");
        this.positionsLimit = positionsLimit;
        this.fpp = fpp;

        this.builders = new ImmutableList.Builder[channels.size()];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = ImmutableList.builder();
        }
    }

    // Just for test
    public List<String> getValues()
    {
        return values;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        current = page;

        processPageWithBloomfilter(page);
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;

        try {
            sendBloomfilter();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finishing;
    }

    private void processPageWithBloomfilter(Page page)
    {
        if (channels.size() == 0) {
            return;
        }

        positionCount += page.getPositionCount();
        // we don't want to filter if there're too many values
        if (positionCount > positionsLimit) {
            return;
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int channelIndex = 0; channelIndex < channels.size(); channelIndex++) {
                Integer channel = channels.get(channelIndex).getIndex();
                Object object = channels.get(channelIndex).getType().getObjectValue(session.getSqlFunctionProperties(), page.getBlock(channel), position);
                if (object == null) {
                    continue;
                }
                else {
                    stringBuilder.append(object.toString());
                }
            }
            values.add(stringBuilder.toString());
        }
    }

    private void sendBloomfilter() throws IOException
    {
        // we don't want to filter if there're too many values
        if (positionCount > positionsLimit) {
            client.storeSummary(new DynamicFilterSummary(new BloomFilterForDynamicFilterImpl(null, null, 0)));
            return;
        }

        BloomFilterForDynamicFilter myBloomFilter = new BloomFilterForDynamicFilterImpl(BloomFilter.create(positionsLimit, fpp), null, 0);
        for (int i = 0; i < values.size(); i++) {
            myBloomFilter.put(values.get(i));
        }

        client.storeSummary(new DynamicFilterSummary(myBloomFilter));
    }
}
