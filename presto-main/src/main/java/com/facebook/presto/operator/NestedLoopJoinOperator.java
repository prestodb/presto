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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
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
        private final NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier;
        private final List<Type> probeTypes;
        private final List<Type> types;
        private boolean closed;

        public NestedLoopJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier, List<Type> probeTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinPagesSupplier = nestedLoopJoinPagesSupplier;
            this.nestedLoopJoinPagesSupplier.retain();
            this.probeTypes = probeTypes;
            this.types = ImmutableList.<Type>builder()
                    .addAll(probeTypes)
                    .addAll(nestedLoopJoinPagesSupplier.getTypes())
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopJoinOperator.class.getSimpleName());
            return new NestedLoopJoinOperator(operatorContext, nestedLoopJoinPagesSupplier, probeTypes);
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            nestedLoopJoinPagesSupplier.release();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopJoinOperatorFactory(operatorId, planNodeId, nestedLoopJoinPagesSupplier, probeTypes);
        }
    }

    private final NestedLoopJoinPagesSupplier buildPagesSupplier;
    private final ListenableFuture<NestedLoopJoinPages> nestedLoopJoinPagesFuture;

    private final OperatorContext operatorContext;
    private final List<Type> types;

    private List<Page> buildPages;
    private Page probePage;
    private Iterator<Page> buildPageIterator;
    private NestedLoopPageBuilder nestedLoopPageBuilder;
    private boolean finishing;
    private boolean closed;

    public NestedLoopJoinOperator(OperatorContext operatorContext, NestedLoopJoinPagesSupplier buildPagesSupplier, List<Type> probeTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.buildPagesSupplier = requireNonNull(buildPagesSupplier, "buildPagesSupplier is null");
        this.nestedLoopJoinPagesFuture = buildPagesSupplier.getPagesFuture();
        buildPagesSupplier.retain();

        requireNonNull(probeTypes, "probeTypes is null");
        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(buildPagesSupplier.getTypes())
                .build();
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

        if (nestedLoopPageBuilder != null && nestedLoopPageBuilder.hasNext()) {
            return nestedLoopPageBuilder.next();
        }

        if (buildPageIterator.hasNext()) {
            nestedLoopPageBuilder = new NestedLoopPageBuilder(probePage, buildPageIterator.next());
            return nestedLoopPageBuilder.next();
        }

        probePage = null;
        return null;
    }

    @Override
    public void close()
    {
        buildPages = null;
        // We don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        buildPagesSupplier.release();
    }

    /**
     * This class takes one probe page(p rows) and one build page(b rows) and
     * build n pages with m rows in each page, where n = min(p, b) and m = max(p, b)
     */
    @VisibleForTesting
    static class NestedLoopPageBuilder
            implements Iterator<Page>
    {
        private final int numberOfProbeColumns;
        private final int numberOfBuildColumns;
        private final boolean buildPageLarger;
        private final Page largePage;
        private final Page smallPage;
        private final int maxRowIndex; // number of rows - 1

        private int rowIndex; // Iterator on the rows in the page with less rows.
        private final int noColumnShortcutResult; // Only used if select count(*) from cross join.

        NestedLoopPageBuilder(Page probePage, Page buildPage)
        {
            requireNonNull(probePage, "probePage is null");
            checkArgument(probePage.getPositionCount() > 0, "probePage has no rows");
            requireNonNull(buildPage, "buildPage is null");
            checkArgument(buildPage.getPositionCount() > 0, "buildPage has no rows");
            this.numberOfProbeColumns = probePage.getChannelCount();
            this.numberOfBuildColumns = buildPage.getChannelCount();

            // We will loop through all rows in the page with less rows.
            this.rowIndex = -1;
            this.buildPageLarger = buildPage.getPositionCount() > probePage.getPositionCount();
            this.maxRowIndex = Math.min(buildPage.getPositionCount(), probePage.getPositionCount()) - 1;
            this.largePage = buildPageLarger ? buildPage : probePage;
            this.smallPage = buildPageLarger ? probePage : buildPage;

            this.noColumnShortcutResult = calculateUseNoColumnShortcut(numberOfProbeColumns, numberOfBuildColumns, probePage.getPositionCount(), buildPage.getPositionCount());
        }

        private static int calculateUseNoColumnShortcut(
                int numberOfProbeColumns,
                int numberOfBuildColumns,
                int positionCountProbe,
                int positionCountBuild)
        {
            if (numberOfProbeColumns == 0 && numberOfBuildColumns == 0) {
                try {
                    // positionCount is an int. Make sure the product can still fit in an int.
                    return multiplyExact(positionCountProbe, positionCountBuild);
                }
                catch (ArithmeticException exception) {
                    // return -1 to disable the shortcut if overflows.
                }
            }
            return -1;
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

            if (noColumnShortcutResult >= 0) {
                rowIndex = maxRowIndex;
                return new Page(noColumnShortcutResult);
            }

            rowIndex++;

            // Create an array of blocks for all columns in both pages.
            Block[] blocks = new Block[numberOfProbeColumns + numberOfBuildColumns];

            // Make sure we always put the probe data on the left and build data on the right.
            int indexForRleBlocks = buildPageLarger ? 0 : numberOfProbeColumns;
            int indexForPageBlocks = buildPageLarger ? numberOfProbeColumns : 0;

            // For the page with less rows, create RLE blocks and add them to the blocks array
            for (int i = 0; i < smallPage.getChannelCount(); i++) {
                Block block = smallPage.getBlock(i).getSingleValueBlock(rowIndex);
                blocks[indexForRleBlocks] = new RunLengthEncodedBlock(block, largePage.getPositionCount());
                indexForRleBlocks++;
            }

            // Put the page with more rows in the blocks array
            System.arraycopy(largePage.getBlocks(), 0, blocks, indexForPageBlocks, largePage.getChannelCount());

            return new Page(largePage.getPositionCount(), blocks);
        }
    }
}
