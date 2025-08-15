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
import com.facebook.presto.operator.project.PageProcessor;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class NestedLoopJoinPagesBuilder
{
    private final OperatorContext operatorContext;
    private int emptyChannelPositionCounter;
    private List<Page> pages;
    private boolean finished;

    private long estimatedSize;

    NestedLoopJoinPagesBuilder(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pages = new ArrayList<>();
    }

    public void addPage(Page page)
    {
        checkNotFinished();

        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        // Fast path for empty output channels
        if (page.getChannelCount() == 0) {
            updatePagePositionCounter(page.getPositionCount());
            return;
        }

        pages.add(page);
        estimatedSize += page.getRetainedSizeInBytes();
    }

    private void updatePagePositionCounter(int positions)
    {
        // Overflow should not be possible here since both arguments start as ints
        long nextPositionCount = addExact(this.emptyChannelPositionCounter, (long) positions);
        while (nextPositionCount >= PageProcessor.MAX_BATCH_SIZE) {
            nextPositionCount -= PageProcessor.MAX_BATCH_SIZE;
            Page flushed = new Page(PageProcessor.MAX_BATCH_SIZE);
            pages.add(flushed);
            estimatedSize += flushed.getRetainedSizeInBytes();
        }
        // Overflow should not occur since MAX_BATCH_SIZE is itself a positive integer
        this.emptyChannelPositionCounter = toIntExact(nextPositionCount);
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        checkNotFinished();
        long estimatedSize = 0L;
        for (Page page : pages) {
            page.compact();
            estimatedSize += page.getRetainedSizeInBytes();
        }
        this.estimatedSize = estimatedSize;
    }

    public NestedLoopJoinPages build()
    {
        checkNotFinished();

        // Flush the position counter if we're in empty channels mode
        if (emptyChannelPositionCounter > 0) {
            Page output = new Page(emptyChannelPositionCounter);
            pages.add(output);
            estimatedSize += output.getRetainedSizeInBytes();
            this.emptyChannelPositionCounter = 0;
        }

        finished = true;
        pages = ImmutableList.copyOf(pages);
        return new NestedLoopJoinPages(pages, getEstimatedSize(), operatorContext);
    }

    private void checkNotFinished()
    {
        checkState(!finished, "NestedLoopJoinPagesBuilder is already finished");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedSize)
                .add("pageCount", pages.size())
                .toString();
    }
}
