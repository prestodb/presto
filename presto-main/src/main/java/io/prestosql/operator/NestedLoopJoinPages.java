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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class NestedLoopJoinPages
{
    private final ImmutableList<Page> pages;
    private final DataSize estimatedSize;

    NestedLoopJoinPages(List<Page> pages, DataSize estimatedSize, OperatorContext operatorContext)
    {
        requireNonNull(pages, "pages is null");
        requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(estimatedSize, "estimatedSize is null");
        this.pages = ImmutableList.copyOf(pages);
        this.estimatedSize = estimatedSize;
    }

    public List<Page> getPages()
    {
        return pages;
    }

    public DataSize getEstimatedSize()
    {
        return estimatedSize;
    }
}
