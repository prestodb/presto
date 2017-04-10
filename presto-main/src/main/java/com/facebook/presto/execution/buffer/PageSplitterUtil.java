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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class PageSplitterUtil
{
    private PageSplitterUtil() {}

    public static List<Page> splitPage(Page page, long maxPageSizeInBytes)
    {
        checkArgument(page.getPositionCount() > 0, "page is empty");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be > 0");

        if (page.getSizeInBytes() <= maxPageSizeInBytes || page.getPositionCount() == 1) {
            return ImmutableList.of(page);
        }

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        int positionCount = page.getPositionCount();
        int half = positionCount / 2;

        Page splitPage1 = page.getRegion(0, half);
        outputPages.addAll(splitPage(splitPage1, maxPageSizeInBytes));

        Page splitPage2 = page.getRegion(half, positionCount - half);
        outputPages.addAll(splitPage(splitPage2, maxPageSizeInBytes));

        return outputPages.build();
    }
}
