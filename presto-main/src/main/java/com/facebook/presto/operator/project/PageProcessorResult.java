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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PageProcessorResult
{
    public static final PageProcessorResult EMPTY_PAGE_PROCESSOR_RESULT = new PageProcessorResult();

    private final Optional<Page> page;

    public PageProcessorResult(Page page)
    {
        this.page = Optional.of(requireNonNull(page, "page is null"));
    }

    private PageProcessorResult()
    {
        this.page = Optional.empty();
    }

    public Page getPage()
    {
        return page.orElseThrow(() -> new AssertionError("page is not present"));
    }

    public boolean hasPage()
    {
        return page.isPresent();
    }
}
