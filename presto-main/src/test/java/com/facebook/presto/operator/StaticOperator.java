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

import com.facebook.presto.block.Block;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StaticOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final ImmutableList<TupleInfo> tupleInfos;
    private final Iterator<Page> pages;

    public StaticOperator(OperatorContext operatorContext, List<Page> pages)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(pages, "pages is null");
        checkArgument(!pages.isEmpty(), "pages is empty");

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (Block block : pages.get(0).getBlocks()) {
            tupleInfos.add(block.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
        this.pages = ImmutableList.copyOf(pages).iterator();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        Iterators.size(pages);
    }

    @Override
    public boolean isFinished()
    {
        return !pages.hasNext();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
        if (!pages.hasNext()) {
            return null;
        }
        Page page = pages.next();
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount());
        }
        return page;
    }
}
