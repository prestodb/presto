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
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class BufferResult
{
    public static BufferResult emptyResults(long endSequenceId, boolean bufferClosed)
    {
        return new BufferResult(endSequenceId, bufferClosed, ImmutableList.<Page>of());
    }

    public static BufferResult bufferResult(long startingSequenceId, Page firstElement, Page... otherElements)
    {
        return new BufferResult(startingSequenceId, false, ImmutableList.<Page>builder().add(firstElement).add(otherElements).build());
    }

    private final long startingSequenceId;
    private final boolean bufferClosed;
    private final List<Page> elements;

    public BufferResult(long startingSequenceId, boolean bufferClosed, List<Page> elements)
    {
        this.startingSequenceId = startingSequenceId;
        this.bufferClosed = bufferClosed;
        this.elements = ImmutableList.copyOf(checkNotNull(elements, "pages is null"));
    }

    public long getStartingSequenceId()
    {
        return startingSequenceId;
    }

    public boolean isBufferClosed()
    {
        return bufferClosed;
    }

    public List<Page> getElements()
    {
        return elements;
    }

    public int size()
    {
        return elements.size();
    }

    public boolean isEmpty()
    {
        return elements.isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bufferClosed, elements);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BufferResult other = (BufferResult) obj;
        return Objects.equal(this.bufferClosed, other.bufferClosed) && Objects.equal(this.elements, other.elements);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferClosed", bufferClosed)
                .add("elements", elements)
                .toString();
    }
}
