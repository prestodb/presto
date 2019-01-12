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
package io.prestosql.spi;

import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;

import java.util.List;

public interface PageSorter
{
    /**
     * @return Sorted synthetic addresses for pages. A synthetic address is encoded as a long with
     * the high 32 bits containing the page index and the low 32 bits containing position index
     */
    @Deprecated
    long[] sort(List<Type> types, List<Page> pages, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions);

    int decodePageIndex(long address);

    int decodePositionIndex(long address);
}
