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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class NestedLoopJoinPagesBuilder
{
    private final OperatorContext operatorContext;
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
        checkState(!finished, "NestedLoopJoinPagesBuilder is finished");

        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        pages.add(page);
        estimatedSize += page.getRetainedSizeInBytes();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        checkState(!finished, "NestedLoopJoinPagesBuilder is finished");

        pages.stream()
                .forEach(Page::compact);
        estimatedSize = pages.stream()
                .mapToLong(Page::getRetainedSizeInBytes)
                .sum();
    }

    public NestedLoopJoinPages build()
    {
        checkState(!finished, "NestedLoopJoinPagesBuilder is already finished");

        finished = true;
        pages = ImmutableList.copyOf(pages);
        return new NestedLoopJoinPages(pages, getEstimatedSize(), operatorContext);
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
