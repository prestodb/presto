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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final SettableLookupSourceSupplier lookupSourceSupplier;
        private final List<Integer> hashChannels;
        private final Optional<Integer> hashChannel;

        private final int expectedPositions;
        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                List<Type> types,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.lookupSourceSupplier = new SettableLookupSourceSupplier(requireNonNull(types, "types is null"));

            Preconditions.checkArgument(!hashChannels.isEmpty(), "hashChannels is empty");
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceSupplier getLookupSourceSupplier()
        {
            return lookupSourceSupplier;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceSupplier.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashBuilderOperator.class.getSimpleName());
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceSupplier,
                    hashChannels,
                    hashChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final SettableLookupSourceSupplier lookupSourceSupplier;
    private final List<Integer> hashChannels;
    private final Optional<Integer> hashChannel;

    private final PagesIndex pagesIndex;

    private boolean finished;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            SettableLookupSourceSupplier lookupSourceSupplier,
            List<Integer> hashChannels,
            Optional<Integer> hashChannel,
            int expectedPositions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "hashSupplier is null");

        Preconditions.checkArgument(!hashChannels.isEmpty(), "hashChannels is empty");
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");

        this.pagesIndex = new PagesIndex(lookupSourceSupplier.getTypes(), expectedPositions);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceSupplier.getTypes();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        // After this point the SharedLookupSource will take over our memory reservation, and ours will be zero
        lookupSourceSupplier.setLookupSource(new SharedLookupSource(pagesIndex.createLookupSource(hashChannels, hashChannel), operatorContext));
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        pagesIndex.addPage(page);
        if (!operatorContext.trySetMemoryReservation(pagesIndex.getEstimatedSize().toBytes())) {
            pagesIndex.compact();
        }
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
