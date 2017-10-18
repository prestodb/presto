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
package com.facebook.presto.operator.index;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MappedPageSet
        implements PageSet
{
    private final PageSet delegate;
    private final List<Type> delegateColumnTypes;
    private final int[] delegateFieldIndex;

    public MappedPageSet(PageSet delegate, List<Integer> delegateFieldIndex)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateFieldIndex = Ints.toArray(requireNonNull(delegateFieldIndex, "delegateFieldIndex is null"));
        this.delegateColumnTypes = Arrays.stream(this.delegateFieldIndex).mapToObj(channel -> delegate.getColumnTypes().get(channel)).collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return delegateColumnTypes;
    }

    @Override
    public List<Page> getPages()
    {
        return delegate.getPages().stream().map(page -> getPage(page)).collect(toImmutableList());
    }

    private Page getPage(Page page)
    {
        Block[] blocks = Arrays.stream(delegateFieldIndex)
                .mapToObj(page::getBlock)
                .toArray(Block[]::new);
        return new Page(page.getPositionCount(), blocks);
    }
}
