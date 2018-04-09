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
package com.facebook.presto.operator.project;

import com.facebook.presto.spi.Page;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class PageProcessorOutput
        implements Iterator<Optional<Page>>
{
    public static final PageProcessorOutput EMPTY_PAGE_PROCESSOR_OUTPUT = new PageProcessorOutput(() -> 0, emptyIterator());

    private final LongSupplier retainedSizeInBytesSupplier;
    private final Iterator<Optional<Page>> pages;
    private long retainedSizeInBytes;

    public PageProcessorOutput(LongSupplier retainedSizeInBytesSupplier, Iterator<Optional<Page>> pages)
    {
        this.retainedSizeInBytesSupplier = requireNonNull(retainedSizeInBytesSupplier, "retainedSizeInBytesSupplier is null");
        this.pages = requireNonNull(pages, "pages is null");
        this.retainedSizeInBytes = retainedSizeInBytesSupplier.getAsLong();
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public boolean hasNext()
    {
        boolean result = pages.hasNext();
        retainedSizeInBytes = retainedSizeInBytesSupplier.getAsLong();
        return result;
    }

    @Override
    public Optional<Page> next()
    {
        return pages.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
