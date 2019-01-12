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
package io.prestosql.rcfile;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Copy of io.prestosql.execution.buffer.PageSplitterUtil
 */
public final class PageSplitterUtil
{
    private PageSplitterUtil() {}

    public static List<Page> splitPage(Page page, long maxPageSizeInBytes)
    {
        return splitPage(page, maxPageSizeInBytes, Long.MAX_VALUE);
    }

    private static List<Page> splitPage(Page page, long maxPageSizeInBytes, long previousPageSize)
    {
        checkArgument(page.getPositionCount() > 0, "page is empty");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be > 0");

        // for Pages with certain types of Blocks (e.g., RLE blocks) the size in bytes may remain constant
        // through the recursive calls, which causes the recursion to only terminate when page.getPositionCount() == 1
        // and create potentially a large number of Page's of size 1. So we check here that
        // if the size of the page doesn't improve from the previous call we terminate the recursion.
        if (page.getSizeInBytes() == previousPageSize || page.getSizeInBytes() <= maxPageSizeInBytes || page.getPositionCount() == 1) {
            return ImmutableList.of(page);
        }

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        long previousSize = page.getSizeInBytes();
        int positionCount = page.getPositionCount();
        int half = positionCount / 2;

        Page leftHalf = page.getRegion(0, half);
        outputPages.addAll(splitPage(leftHalf, maxPageSizeInBytes, previousSize));

        Page rightHalf = page.getRegion(half, positionCount - half);
        outputPages.addAll(splitPage(rightHalf, maxPageSizeInBytes, previousSize));

        return outputPages.build();
    }
}
