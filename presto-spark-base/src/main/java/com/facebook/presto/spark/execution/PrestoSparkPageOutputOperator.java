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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spark.util.PrestoSparkUtils.createPagesSerde;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkPageOutputOperator
        implements Operator
{
    public static class PrestoSparkPageOutputFactory
            implements OutputFactory
    {
        private final PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer;
        private final BlockEncodingManager blockEncodingManager;

        public PrestoSparkPageOutputFactory(
                PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer,
                BlockEncodingManager blockEncodingManager)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                Optional<OutputPartitioning> outputPartitioning,
                PagesSerdeFactory serdeFactory)
        {
            checkArgument(!outputPartitioning.isPresent(), "output partitioning is not expected to be present");
            return new PrestoSparkOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    outputBuffer,
                    pagePreprocessor,
                    blockEncodingManager);
        }
    }

    public static class PrestoSparkOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer;
        private final Function<Page, Page> pagePreprocessor;
        private final BlockEncodingManager blockEncodingManager;

        public PrestoSparkOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer,
                Function<Page, Page> pagePreprocessor,
                BlockEncodingManager blockEncodingManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PrestoSparkPageOutputOperator.class.getSimpleName());
            return new PrestoSparkPageOutputOperator(
                    operatorContext,
                    outputBuffer,
                    pagePreprocessor,
                    createPagesSerde(blockEncodingManager));
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PrestoSparkOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    outputBuffer,
                    pagePreprocessor,
                    blockEncodingManager);
        }
    }

    private final OperatorContext operatorContext;
    private final PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer;
    private final Function<Page, Page> pagePreprocessor;
    private final PagesSerde pagesSerde;

    private ListenableFuture<?> isBlocked = NOT_BLOCKED;

    private boolean finished;

    public PrestoSparkPageOutputOperator(
            OperatorContext operatorContext,
            PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer,
            Function<Page, Page> pagePreprocessor,
            PagesSerde pagesSerde)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (isBlocked.isDone()) {
            isBlocked = outputBuffer.isFull();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        page = pagePreprocessor.apply(page);

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        List<PrestoSparkBufferedSerializedPage> serializedPages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                .map(pagesSerde::serialize)
                .map(PrestoSparkBufferedSerializedPage::new)
                .collect(toImmutableList());

        serializedPages.forEach(outputBuffer::enqueue);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }
}
