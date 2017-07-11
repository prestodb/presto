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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class PageSourceOperator
        implements Operator, Closeable
{
    private final ConnectorPageSource pageSource;
    private final List<Type> types;
    private final OperatorContext operatorContext;
    private long completedBytes;
    private long readTimeNanos;

    public PageSourceOperator(ConnectorPageSource pageSource, List<Type> types, OperatorContext operatorContext)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
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
        try {
            pageSource.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        CompletableFuture<?> pageSourceBlocked = pageSource.isBlocked();
        return pageSourceBlocked.isDone() ? NOT_BLOCKED : toListenableFuture(pageSourceBlocked);
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        Page page = pageSource.getNextPage();
        if (page == null) {
            return null;
        }

        // update operator stats
        long endCompletedBytes = pageSource.getCompletedBytes();
        long endReadTimeNanos = pageSource.getReadTimeNanos();
        operatorContext.recordGeneratedInput(endCompletedBytes - completedBytes, page.getPositionCount(), endReadTimeNanos - readTimeNanos);
        completedBytes = endCompletedBytes;
        readTimeNanos = endReadTimeNanos;

        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }
}
