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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.SliceInput;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final Iterator<PrestoSparkRow> iterator;
    private final List<Type> types;

    private boolean finished;

    public PrestoSparkRemoteSourceOperator(PlanNodeId sourceId, OperatorContext operatorContext, Iterator<PrestoSparkRow> iterator, List<Type> types)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        synchronized (iterator) {
            while (iterator.hasNext() && !pageBuilder.isFull()) {
                PrestoSparkRow row = iterator.next();
                SliceInput sliceInput = new BasicSliceInput(wrappedBuffer(row.getBytes(), 0, row.getLength()));
                pageBuilder.declarePosition();
                for (int channel = 0; channel < types.size(); channel++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                    blockBuilder.readPositionFrom(sliceInput);
                }
                sliceInput.close();
            }

            if (!iterator.hasNext()) {
                finished = true;
            }
        }

        if (pageBuilder.isEmpty()) {
            return null;
        }

        return pageBuilder.build();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreSplits()
    {
        throw new UnsupportedOperationException();
    }

    public static class SparkRemoteSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Iterator<PrestoSparkRow> iterator;
        private final List<Type> types;

        private boolean closed;

        public SparkRemoteSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, Iterator<PrestoSparkRow> iterator, List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.iterator = requireNonNull(iterator, "iterator is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "operator factory is closed");
            return new PrestoSparkRemoteSourceOperator(
                    planNodeId,
                    driverContext.addOperatorContext(operatorId, planNodeId, PrestoSparkRemoteSourceOperator.class.getSimpleName()),
                    iterator,
                    types);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
