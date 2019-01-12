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
package com.facebook.presto;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class PagesIndexPageSorter
        implements PageSorter
{
    private final PagesIndex.Factory pagesIndexFactory;

    @Inject
    public PagesIndexPageSorter(PagesIndex.Factory pagesIndexFactory)
    {
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
    }

    @Override
    public long[] sort(List<Type> types, List<Page> pages, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions)
    {
        PagesIndex pagesIndex = pagesIndexFactory.newPagesIndex(types, expectedPositions);
        pages.forEach(pagesIndex::addPage);
        pagesIndex.sort(sortChannels, sortOrders);

        return pagesIndex.getValueAddresses().toLongArray(null);
    }

    @Override
    public int decodePageIndex(long address)
    {
        return decodeSliceIndex(address);
    }

    @Override
    public int decodePositionIndex(long address)
    {
        return decodePosition(address);
    }
}
