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

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class BufferResult
{
    public static BufferResult emptyResults(long token, boolean bufferClosed)
    {
        return new BufferResult(token, token, bufferClosed, ImmutableList.<Page>of());
    }

    private final long token;
    private final long nextToken;
    private final boolean bufferClosed;
    private final List<Page> pages;

    public BufferResult(long token, long nextToken, boolean bufferClosed, List<Page> pages)
    {
        this.token = token;
        this.nextToken = nextToken;
        this.bufferClosed = bufferClosed;
        this.pages = ImmutableList.copyOf(checkNotNull(pages, "pages is null"));
    }

    public long getToken()
    {
        return token;
    }

    public long getNextToken()
    {
        return nextToken;
    }

    public boolean isBufferClosed()
    {
        return bufferClosed;
    }

    public List<Page> getPages()
    {
        return pages;
    }

    public int size()
    {
        return pages.size();
    }

    public boolean isEmpty()
    {
        return pages.isEmpty();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferResult that = (BufferResult) o;
        return Objects.equals(token, that.token) &&
                Objects.equals(nextToken, that.nextToken) &&
                Objects.equals(bufferClosed, that.bufferClosed) &&
                Objects.equals(pages, that.pages);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, nextToken, bufferClosed, pages);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", token)
                .add("nextToken", nextToken)
                .add("bufferClosed", bufferClosed)
                .add("pages", pages)
                .toString();
    }
}
