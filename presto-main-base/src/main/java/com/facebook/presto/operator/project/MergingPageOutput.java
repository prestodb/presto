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
package com.facebook.presto.operator.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * This class is intended to be used right after the PageProcessor to ensure
 * that the size of the pages returned by FilterAndProject and ScanFilterAndProject
 * is big enough so it does not introduce considerable synchronization overhead.
 * <p>
 * As long as the input page contains more than {@link MergingPageOutput#minRowCount} rows
 * or is bigger than {@link MergingPageOutput#minPageSizeInBytes} it is returned as is without
 * additional memory copy.
 * <p>
 * The page data that has been buffered so far before receiving a "big" page is being flushed
 * before transferring a "big" page.
 * <p>
 * Although it is still possible that the {@link MergingPageOutput} may return a tiny page,
 * this situation is considered to be rare due to the assumption that filter selectivity may not
 * vary a lot based on the particular input page.
 * <p>
 * Considering the CPU time required to process(filter, project) a full (~1MB) page returned by a
 * connector, the CPU cost of memory copying (< 50kb, < 1024 rows) is supposed to be negligible.
 */
@NotThreadSafe
public class MergingPageOutput
{
    @VisibleForTesting
    static final int INSTANCE_SIZE = ClassLayout.parseClass(MergingPageOutput.class).instanceSize();
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

    private final List<Type> types;
    @Nullable
    private final PageBuilder pageBuilder; // when null, only page position counts are output
    private final Queue<Page> outputQueue = new LinkedList<>();

    private final long minPageSizeInBytes;
    private final int minRowCount;
    private int pendingPositionCount;

    @Nullable
    private Iterator<Optional<Page>> currentInput;
    private boolean finishing;

    public MergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount)
    {
        this(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    public MergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount, int maxPageSizeInBytes)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
        checkArgument(minRowCount <= MAX_BATCH_SIZE, "minRowCount must be less than or equal to %s", MAX_BATCH_SIZE);
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
        checkArgument(maxPageSizeInBytes >= minPageSizeInBytes, "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
        checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %d", MAX_MIN_PAGE_SIZE);
        this.minPageSizeInBytes = minPageSizeInBytes;
        this.minRowCount = minRowCount;
        if (this.types.isEmpty()) {
            pageBuilder = null; // position count only mode
        }
        else {
            pageBuilder = PageBuilder.withMaxPageSize(maxPageSizeInBytes, this.types);
        }
    }

    public boolean needsInput()
    {
        return currentInput == null && !finishing && outputQueue.isEmpty();
    }

    public void addInput(Iterator<Optional<Page>> input)
    {
        requireNonNull(input, "input is null");
        checkState(!finishing, "output is in finishing state");
        checkState(currentInput == null, "currentInput is present");
        currentInput = input;
    }

    @Nullable
    public Page getOutput()
    {
        if (isPositionCountOnly()) {
            return producePositionCountOnlyOutput();
        }

        if (!outputQueue.isEmpty()) {
            return outputQueue.poll();
        }

        while (currentInput != null) {
            if (!currentInput.hasNext()) {
                currentInput = null;
                break;
            }

            if (!outputQueue.isEmpty()) {
                break;
            }

            Optional<Page> next = currentInput.next();
            if (next.isPresent()) {
                process(next.get());
            }
            else {
                break;
            }
        }

        if (currentInput == null && finishing) {
            flush();
        }

        return outputQueue.poll();
    }

    public void finish()
    {
        finishing = true;
    }

    public boolean isFinished()
    {
        return finishing && currentInput == null && outputQueue.isEmpty() && pendingPositionCount == 0 && (isPositionCountOnly() || pageBuilder.isEmpty());
    }

    private boolean isPositionCountOnly()
    {
        return pageBuilder == null;
    }

    /**
     * Specialized implementation of {@link MergingPageOutput#getOutput()} that:
     * 1. Doesn't use the {@link MergingPageOutput#pageBuilder} because we can accumulate small pages in an integer field
     * 2. Can arbitrarily reorder output pages with no columns and therefore does not need to flush small accumulated input
     *    values when a sufficiently large page is encountered
     * 3. Will periodically flush accumulated positionCounts once a combined sum of {@link PageProcessor#MAX_BATCH_SIZE} is
     *    reached, this avoids creating pages with huge position counts that might harm downstream operators
     * 4. As a consequence of the above, will always produce either 0 or 1 output pages, which means it doesn't need or use {@link MergingPageOutput#outputQueue}
     */
    @Nullable
    private Page producePositionCountOnlyOutput()
    {
        while (currentInput != null) {
            if (!currentInput.hasNext()) {
                currentInput = null;
                break;
            }

            Optional<Page> next = currentInput.next();
            if (next.isPresent()) {
                Page nextPage = next.get();
                if (nextPage.getPositionCount() >= MAX_BATCH_SIZE) {
                    // Return pages exceeding the target size directly without accumulating
                    return nextPage;
                }
                // Accumulate pending positions for small pages
                pendingPositionCount += nextPage.getPositionCount();
                if (nextPage.getPositionCount() >= minRowCount || pendingPositionCount >= MAX_BATCH_SIZE) {
                    // Produce a combined positionCount output when individual pages meet the minRowCount
                    // or when accumulated pending positions has reached our output size limit
                    int outputPositions = min(pendingPositionCount, MAX_BATCH_SIZE);
                    pendingPositionCount -= outputPositions;
                    return new Page(outputPositions);
                }
            }
            else {
                break; // yield triggered
            }
        }

        if (currentInput == null && finishing && pendingPositionCount > 0) {
            // Flush the remaining output positions
            Page result = new Page(pendingPositionCount);
            pendingPositionCount = 0;
            return result;
        }

        return null;
    }

    private void process(Page page)
    {
        requireNonNull(page, "page is null");

        int inputPositions = page.getPositionCount();
        if (inputPositions == 0) {
            return;
        }

        // avoid memory copying for pages that are big enough
        if (page.getSizeInBytes() >= minPageSizeInBytes || inputPositions >= minRowCount) {
            flush();
            outputQueue.add(page);
            return;
        }

        buffer(page);
    }

    private void buffer(Page page)
    {
        checkArgument(!isPositionCountOnly(), "position count only pages should not be buffered");
        pageBuilder.declarePositions(page.getPositionCount());
        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            Block block = page.getBlock(channel);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
            for (int position = 0; position < page.getPositionCount(); position++) {
                type.appendTo(block, position, blockBuilder);
            }
        }
        if (pageBuilder.isFull()) {
            flush();
        }
    }

    private void flush()
    {
        if (!isPositionCountOnly() && !pageBuilder.isEmpty()) {
            Page output = pageBuilder.build();
            pageBuilder.reset();
            outputQueue.add(output);
        }
    }

    public long getRetainedSizeInBytes()
    {
        if (isPositionCountOnly()) {
            return INSTANCE_SIZE; // position count only does not use the pageBuilder or outputQueue
        }
        long retainedSizeInBytes = INSTANCE_SIZE + pageBuilder.getRetainedSizeInBytes();
        for (Page page : outputQueue) {
            retainedSizeInBytes += page.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }
}
