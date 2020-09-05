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
package com.facebook.presto.spi;

public class PageSinkContext
{
    private static final PageSinkContext DEFAULT_PAGE_SINK_CONTEXT = PageSinkContext.builder().build();

    private final boolean commitRequired;

    private PageSinkContext(boolean commitRequired)
    {
        this.commitRequired = commitRequired;
    }

    public static PageSinkContext defaultContext()
    {
        return DEFAULT_PAGE_SINK_CONTEXT;
    }

    public boolean isCommitRequired()
    {
        return commitRequired;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private boolean commitRequired;

        public Builder setCommitRequired(boolean commitRequired)
        {
            this.commitRequired = commitRequired;
            return this;
        }

        public PageSinkContext build()
        {
            return new PageSinkContext(commitRequired);
        }
    }
}
