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

import com.facebook.presto.array.AdaptiveLongBigArray;
import com.facebook.presto.common.Page;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class StandardJoinFilterFunction
        implements JoinFilterFunction
{
    private static final Page EMPTY_PAGE = new Page(0);

    private final InternalJoinFilterFunction filterFunction;
    private final AdaptiveLongBigArray addresses;
    private final List<Page> pages;

    public StandardJoinFilterFunction(InternalJoinFilterFunction filterFunction, AdaptiveLongBigArray addresses, List<Page> pages)
    {
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
    }

    @Override
    public boolean filter(int leftPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = addresses.get(leftPosition);
        int pageIndex = decodeSliceIndex(pageAddress);
        int pagePosition = decodePosition(pageAddress);

        return filterFunction.filter(pagePosition, pages.isEmpty() ? EMPTY_PAGE : pages.get(pageIndex), rightPosition, rightPage);
    }
}
