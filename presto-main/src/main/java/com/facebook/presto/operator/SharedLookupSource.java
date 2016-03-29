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
import com.facebook.presto.spi.PageBuilder;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class SharedLookupSource
        implements LookupSource
{
    private final TaskContext taskContext;
    private final LookupSource lookupSource;
    @GuardedBy("this")
    private boolean freed;

    public SharedLookupSource(LookupSource lookupSource, OperatorContext operatorContext)
    {
        requireNonNull(operatorContext, "operatorContext is null");
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
        this.taskContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext();
        operatorContext.transferMemoryToTaskContext(lookupSource.getInMemorySizeInBytes());
    }

    @Override
    public int getChannelCount()
    {
        return lookupSource.getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        return lookupSource.getJoinPositionCount();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return lookupSource.getInMemorySizeInBytes();
    }

    @Override
    public long getJoinPosition(int position, Page page, long rawHash)
    {
        return lookupSource.getJoinPosition(position, page, rawHash);
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        return lookupSource.getJoinPosition(position, page);
    }

    @Override
    public long getNextJoinPosition(long currentPosition)
    {
        return lookupSource.getNextJoinPosition(currentPosition);
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        lookupSource.appendTo(position, pageBuilder, outputChannelOffset);
    }

    synchronized void freeMemory()
    {
        checkState(!freed, "Already freed");
        freed = true;
        taskContext.freeMemory(lookupSource.getInMemorySizeInBytes());
    }

    @Override
    public void close()
    {
        lookupSource.close();
    }
}
