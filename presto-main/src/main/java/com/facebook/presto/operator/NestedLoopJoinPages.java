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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class NestedLoopJoinPages
{
    private final LocalMemoryContext transferredBytesMemoryContext;
    private final ImmutableList<Page> pages;
    @GuardedBy("this")
    private boolean freed;

    NestedLoopJoinPages(List<Page> pages, DataSize estimatedSize, OperatorContext operatorContext)
    {
        requireNonNull(pages, "pages is null");
        requireNonNull(operatorContext, "operatorContext is null");
        this.pages = ImmutableList.copyOf(pages);
        this.transferredBytesMemoryContext = operatorContext.getDriverContext()
                .getPipelineContext()
                .getTaskContext()
                .createNewTransferredBytesMemoryContext();
        operatorContext.transferMemoryToTaskContext(estimatedSize.toBytes(), transferredBytesMemoryContext);
    }

    public List<Page> getPages()
    {
        return pages;
    }

    synchronized void freeMemory()
    {
        checkState(!freed, "Memory already freed");
        freed = true;
        transferredBytesMemoryContext.close();
    }
}
