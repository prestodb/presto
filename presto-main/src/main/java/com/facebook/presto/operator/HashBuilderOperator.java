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

import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static final class HashSupplier
    {
        private final List<Type> types;
        private final SettableFuture<JoinHash> hashFuture = SettableFuture.create();

        public HashSupplier(List<Type> types)
        {
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public ListenableFuture<JoinHash> getSourceHash()
        {
            return hashFuture;
        }

        void setHash(JoinHash joinHash)
        {
            boolean wasSet = hashFuture.set(joinHash);
            checkState(wasSet, "Hash already set");
        }
    }

    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final HashSupplier hashSupplier;
        private final List<Integer> hashChannels;
        private final int expectedPositions;
        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                List<Type> types,
                List<Integer> hashChannels,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.hashSupplier = new HashSupplier(checkNotNull(types, "types is null"));

            Preconditions.checkArgument(!hashChannels.isEmpty(), "hashChannels is empty");
            this.hashChannels = ImmutableList.copyOf(checkNotNull(hashChannels, "hashChannels is null"));

            this.expectedPositions = checkNotNull(expectedPositions, "expectedPositions is null");
        }

        public HashSupplier getHashSupplier()
        {
            return hashSupplier;
        }

        @Override
        public List<Type> getTypes()
        {
            return hashSupplier.types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashBuilderOperator.class.getSimpleName());
            return new HashBuilderOperator(
                    operatorContext,
                    hashSupplier,
                    hashChannels,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final HashSupplier hashSupplier;
    private final List<Integer> hashChannels;

    private final PagesIndex pagesIndex;

    private boolean finished;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            HashSupplier hashSupplier,
            List<Integer> hashChannels,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        this.hashSupplier = checkNotNull(hashSupplier, "hashSupplier is null");

        Preconditions.checkArgument(!hashChannels.isEmpty(), "hashChannels is empty");
        this.hashChannels = ImmutableList.copyOf(checkNotNull(hashChannels, "hashChannels is null"));

        this.pagesIndex = new PagesIndex(hashSupplier.getTypes(), expectedPositions, operatorContext);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return hashSupplier.getTypes();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        JoinHash joinHash = pagesIndex.createJoinHash(hashChannels, operatorContext);
        hashSupplier.setHash(joinHash);
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        pagesIndex.addPage(page);
        operatorContext.recordGeneratedOutput(page.getDataSize(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
