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
package com.facebook.presto.operator.index;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.FinishedOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageSourceOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IndexSourceOperator
        implements SourceOperator
{
    public static class IndexSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ConnectorIndex index;
        private final List<Type> types;
        private final Function<RecordSet, RecordSet> probeKeyNormalizer;
        private boolean closed;

        public IndexSourceOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ConnectorIndex index,
                List<Type> types,
                Function<RecordSet, RecordSet> probeKeyNormalizer)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.index = requireNonNull(index, "index is null");
            this.types = requireNonNull(types, "types is null");
            this.probeKeyNormalizer = requireNonNull(probeKeyNormalizer, "probeKeyNormalizer is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, IndexSourceOperator.class.getSimpleName());
            return new IndexSourceOperator(
                    operatorContext,
                    sourceId,
                    index,
                    types,
                    probeKeyNormalizer);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final ConnectorIndex index;
    private final List<Type> types;
    private final Function<RecordSet, RecordSet> probeKeyNormalizer;

    private Operator source;

    public IndexSourceOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            ConnectorIndex index,
            List<Type> types,
            Function<RecordSet, RecordSet> probeKeyNormalizer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.index = requireNonNull(index, "index is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.probeKeyNormalizer = requireNonNull(probeKeyNormalizer, "probeKeyNormalizer is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkType(split.getConnectorSplit(), IndexSplit.class, "connectorSplit");
        checkState(source == null, "Index source split already set");

        IndexSplit indexSplit = (IndexSplit) split.getConnectorSplit();

        // Normalize the incoming RecordSet to something that can be consumed by the index
        RecordSet normalizedRecordSet = probeKeyNormalizer.apply(indexSplit.getKeyRecordSet());
        RecordSet result = index.lookup(normalizedRecordSet);
        source = new PageSourceOperator(new RecordPageSource(result), result.getColumnTypes(), operatorContext);

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        if (source == null) {
            source = new FinishedOperator(operatorContext, types);
        }
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        noMoreSplits();
        source.finish();
    }

    @Override
    public boolean isFinished()
    {
        return (source != null) && source.isFinished();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (source == null) {
            return null;
        }
        return source.getOutput();
    }

    @Override
    public void close()
            throws Exception
    {
        if (source != null) {
            source.close();
        }
    }
}
