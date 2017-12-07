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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MergingPageOutput.class).instanceSize();
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private final Queue<Page> outputQueue = new LinkedList<>();

    private final long minPageSizeInBytes;
    private final int minRowCount;

    @Nullable
    private PageProcessorOutput currentInput;
    private boolean finishing;

    public MergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount)
    {
        this(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    public MergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount, int maxPageSizeInBytes)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
        checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
        checkArgument(maxPageSizeInBytes >= minPageSizeInBytes, "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
        checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %d", MAX_MIN_PAGE_SIZE);
        this.minPageSizeInBytes = minPageSizeInBytes;
        this.minRowCount = minRowCount;
        pageBuilder = PageBuilder.withMaxPageSize(maxPageSizeInBytes, this.types);
    }

    public boolean needsInput()
    {
        return currentInput == null && !finishing && outputQueue.isEmpty();
    }

    public void addInput(PageProcessorOutput input)
    {
        requireNonNull(input, "input is null");
        checkState(!finishing, "output is in finishing state");
        checkState(currentInput == null, "currentInput is present");
        currentInput = input;
    }

    @Nullable
    public Page getOutput()
    {
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
        return finishing && currentInput == null && outputQueue.isEmpty() && pageBuilder.isEmpty();
    }

    private void process(Page page)
    {
        requireNonNull(page, "page is null");

        // avoid memory copying for pages that are big enough
        if (page.getSizeInBytes() >= minPageSizeInBytes || page.getPositionCount() >= minRowCount) {
            flush();
            outputQueue.add(page);
            return;
        }

        buffer(page);
    }

    private void buffer(Page page)
    {
        pageBuilder.declarePositions(page.getPositionCount());
        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            for (int position = 0; position < page.getPositionCount(); position++) {
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }
        if (pageBuilder.isFull()) {
            flush();
        }
    }

    private void flush()
    {
        if (!pageBuilder.isEmpty()) {
            Page output = pageBuilder.build();
            pageBuilder.reset();
            outputQueue.add(output);
        }
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        retainedSizeInBytes += pageBuilder.getRetainedSizeInBytes();
        if (currentInput != null) {
            retainedSizeInBytes += currentInput.getRetainedSizeInBytes();
        }
        for (Page page : outputQueue) {
            retainedSizeInBytes += page.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }
}
