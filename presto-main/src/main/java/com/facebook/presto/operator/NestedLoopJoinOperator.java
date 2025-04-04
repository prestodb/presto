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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;

public class NestedLoopJoinOperator
        implements Operator, Closeable
{
    public static class NestedLoopJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<NestedLoopJoinBridge> joinBridgeManager;
        private boolean closed;

        public NestedLoopJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = nestedLoopJoinBridgeManager;
            this.joinBridgeManager.incrementProbeFactoryCount();
        }

        private NestedLoopJoinOperatorFactory(NestedLoopJoinOperatorFactory other)
        {
            requireNonNull(other, "other is null");
            this.operatorId = other.operatorId;
            this.planNodeId = other.planNodeId;

            this.joinBridgeManager = other.joinBridgeManager;

            // closed is intentionally not copied
            closed = false;

            joinBridgeManager.incrementProbeFactoryCount();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            NestedLoopJoinBridge nestedLoopJoinBridge = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopJoinOperator.class.getSimpleName());

            joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
            return new NestedLoopJoinOperator(
                    operatorContext,
                    nestedLoopJoinBridge,
                    () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()));
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
            joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.probeOperatorFactoryClosed(lifespan);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopJoinOperatorFactory(this);
        }
    }

    private final ListenableFuture<NestedLoopJoinPages> nestedLoopJoinPagesFuture;

    private final OperatorContext operatorContext;
    private final Runnable afterClose;

    private List<Page> buildPages;
    private Page probePage;
    private Iterator<Page> buildPageIterator;
    private NestedLoopOutputIterator nestedLoopOutputIterator;
    private boolean finishing;
    private boolean closed;

    private NestedLoopJoinOperator(OperatorContext operatorContext, NestedLoopJoinBridge joinBridge, Runnable afterClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinPagesFuture = joinBridge.getPagesFuture();
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && probePage == null;

        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return nestedLoopJoinPagesFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || probePage != null) {
            return false;
        }

        if (buildPages == null) {
            Optional<NestedLoopJoinPages> nestedLoopJoinPages = tryGetFutureValue(nestedLoopJoinPagesFuture);
            if (nestedLoopJoinPages.isPresent()) {
                buildPages = nestedLoopJoinPages.get().getPages();
            }
        }
        return buildPages != null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(buildPages != null, "Page source has not been built yet");
        checkState(probePage == null, "Current page has not been completely processed yet");
        checkState(buildPageIterator == null || !buildPageIterator.hasNext(), "Current buildPageIterator has not been completely processed yet");

        if (page.getPositionCount() > 0) {
            probePage = page;
            buildPageIterator = buildPages.iterator();
        }
    }

    @Override
    public Page getOutput()
    {
        // Either probe side or build side is not ready
        if (probePage == null || buildPages == null) {
            return null;
        }

        if (nestedLoopOutputIterator != null && nestedLoopOutputIterator.hasNext()) {
            return nestedLoopOutputIterator.next();
        }

        if (buildPageIterator.hasNext()) {
            nestedLoopOutputIterator = createNestedLoopOutputIterator(probePage, buildPageIterator.next());
            return nestedLoopOutputIterator.next();
        }

        probePage = null;
        nestedLoopOutputIterator = null;
        return null;
    }

    @Override
    public void close()
    {
        buildPages = null;
        probePage = null;
        nestedLoopOutputIterator = null;
        buildPageIterator = null;
        // We don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        // `afterClose` must be run last.
        afterClose.run();
    }

    @VisibleForTesting
    static NestedLoopOutputIterator createNestedLoopOutputIterator(Page probePage, Page buildPage)
    {
        if (probePage.getChannelCount() == 0 && buildPage.getChannelCount() == 0) {
            int probePositions = probePage.getPositionCount();
            int buildPositions = buildPage.getPositionCount();
            try {
                // positionCount is an int. Make sure the product can still fit in an int.
                int outputPositions = multiplyExact(probePositions, buildPositions);
                if (outputPositions <= PageProcessor.MAX_BATCH_SIZE) {
                    return new PageRepeatingIterator(new Page(outputPositions), 1);
                }
            }
            catch (ArithmeticException overflow) {
            }
            // Repeat larger position count a smaller position count number of times
            Page outputPage = new Page(max(probePositions, buildPositions));
            return new PageRepeatingIterator(outputPage, min(probePositions, buildPositions));
        }
        else if (probePage.getChannelCount() == 0 && probePage.getPositionCount() <= buildPage.getPositionCount()) {
            return new PageRepeatingIterator(buildPage, probePage.getPositionCount());
        }
        else if (buildPage.getChannelCount() == 0 && buildPage.getPositionCount() <= probePage.getPositionCount()) {
            return new PageRepeatingIterator(probePage, buildPage.getPositionCount());
        }
        else {
            return new NestedLoopPageBuilder(probePage, buildPage);
        }
    }

    // bi-morphic parent class for the two implementations allowed. Adding a third implementation will make getOutput megamorphic and
    // should be avoided
    @VisibleForTesting
    abstract static class NestedLoopOutputIterator
    {
        public abstract boolean hasNext();

        public abstract Page next();
    }

    private static final class PageRepeatingIterator
            extends NestedLoopOutputIterator
    {
        private final Page page;
        private int remainingCount;

        private PageRepeatingIterator(Page page, int repetitions)
        {
            this.page = requireNonNull(page, "page is null");
            this.remainingCount = repetitions;
        }

        @Override
        public boolean hasNext()
        {
            return remainingCount > 0;
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            remainingCount--;
            return page;
        }
    }

    /**
     * This class takes one probe page(p rows) and one build page(b rows) and
     * build n pages with m rows in each page, where n = min(p, b) and m = max(p, b)
     */
    private static final class NestedLoopPageBuilder
            extends NestedLoopOutputIterator
    {
        //  Avoids allocation a new block array per iteration
        private final Block[] resultBlockBuffer;
        private final Page smallPage;
        private final int indexForRleBlocks;
        private final int largePagePositionCount;
        private final int maxRowIndex; // number of rows - 1

        private int rowIndex = -1; // Iterator on the rows in the page with less rows.

        NestedLoopPageBuilder(Page probePage, Page buildPage)
        {
            requireNonNull(probePage, "probePage is null");
            checkArgument(probePage.getPositionCount() > 0, "probePage has no rows");
            requireNonNull(buildPage, "buildPage is null");
            checkArgument(buildPage.getPositionCount() > 0, "buildPage has no rows");

            Page largePage;
            int indexForPageBlocks;
            if (buildPage.getPositionCount() > probePage.getPositionCount()) {
                largePage = buildPage;
                smallPage = probePage;
                indexForPageBlocks = probePage.getChannelCount();
                this.indexForRleBlocks = 0;
            }
            else {
                largePage = probePage;
                smallPage = buildPage;
                indexForPageBlocks = 0;
                this.indexForRleBlocks = probePage.getChannelCount();
            }
            largePagePositionCount = largePage.getPositionCount();
            maxRowIndex = smallPage.getPositionCount() - 1;

            resultBlockBuffer = new Block[largePage.getChannelCount() + smallPage.getChannelCount()];
            // Put the blocks from the page with more rows in the output buffer
            for (int i = 0; i < largePage.getChannelCount(); i++) {
                resultBlockBuffer[indexForPageBlocks + i] = largePage.getBlock(i);
            }
        }

        @Override
        public boolean hasNext()
        {
            return rowIndex < maxRowIndex;
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            rowIndex++;

            // For the page with less rows, create RLE blocks and add them to the blocks array
            for (int i = 0; i < smallPage.getChannelCount(); i++) {
                Block block = smallPage.getBlock(i).getSingleValueBlock(rowIndex);
                resultBlockBuffer[indexForRleBlocks + i] = new RunLengthEncodedBlock(block, largePagePositionCount);
            }
            // Page constructor will create a copy of the block buffer (and must for correctness)
            return new Page(largePagePositionCount, resultBlockBuffer);
        }
    }
}
