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

import static com.google.common.collect.Iterators.singletonIterator;
import static java.util.Collections.emptyIterator;

public class PageProcessorOutput
        implements Iterator<Page>
{
    public static final PageProcessorOutput EMPTY_PAGE_PROCESSOR_OUTPUT = new PageProcessorOutput(0, emptyIterator());

    private final long retainedSizeInBytes;
    private final Iterator<Page> pages;

    public PageProcessorOutput(Page page)
    {
        this.retainedSizeInBytes = 0;
        this.pages = singletonIterator(page);
    }

    public PageProcessorOutput(long retainedSizeInBytes, Iterator<Page> pages)
    {
        this.retainedSizeInBytes = retainedSizeInBytes;
        this.pages = pages;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public boolean hasNext()
    {
        return pages.hasNext();
    }

    @Override
    public Page next()
    {
        return pages.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
