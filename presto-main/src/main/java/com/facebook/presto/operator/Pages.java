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

import static com.google.common.base.Preconditions.checkPositionIndex;

public final class Pages
{
    private Pages()
    {
    }

    public static int findClusterEnd(Page page, int startPosition, PagePositionEqualitor equalitor)
    {
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");
        return findClusterEnd(page, startPosition, page, startPosition, equalitor);
    }

    public static int findClusterEnd(Page basePage, int basePosition, Page page, int startPosition, PagePositionEqualitor equalitor)
    {
        checkPositionIndex(basePosition, basePage.getPositionCount(), "basePosition out of bounds");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        // Short circuit if the whole page has the same value
        if (equalitor.rowEqualsRow(basePosition, basePage, page.getPositionCount() - 1, page)) {
            return page.getPositionCount();
        }

        // TODO: do position binary search
        int endPosition = startPosition;
        while (endPosition < page.getPositionCount() && equalitor.rowEqualsRow(basePosition, basePage, endPosition, page)) {
            endPosition++;
        }
        // Return endPosition == startPosition if the first row in the page does not match the base
        return endPosition;
    }

    public static int findClusterEnd(PagesIndex pagesIndex, int startPosition, PagesHashStrategy pagesHashStrategy)
    {
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        // Short circuit if the whole pagesIndex has the same value
        if (pagesIndex.positionEqualsPosition(pagesHashStrategy, startPosition, pagesIndex.getPositionCount() - 1)) {
            return pagesIndex.getPositionCount();
        }

        // TODO: do position binary search
        int endPosition = startPosition + 1;
        while ((endPosition < pagesIndex.getPositionCount()) && pagesIndex.positionEqualsPosition(pagesHashStrategy, startPosition, endPosition)) {
            endPosition++;
        }
        return endPosition;
    }
}
