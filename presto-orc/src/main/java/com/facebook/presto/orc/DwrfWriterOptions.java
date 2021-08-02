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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import io.airlift.units.DataSize;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class DwrfWriterOptions
{
    public static final DataSize DEFAULT_STRIPE_CACHE_MAX_SIZE = new DataSize(8, MEGABYTE);
    public static final DwrfStripeCacheMode DEFAULT_STRIPE_CACHE_MODE = INDEX_AND_FOOTER;
    private final boolean stripeCacheEnabled;
    private final DwrfStripeCacheMode stripeCacheMode;
    private final DataSize stripeCacheMaxSize;

    private DwrfWriterOptions(
            boolean stripeCacheEnabled,
            DwrfStripeCacheMode stripeCacheMode,
            DataSize stripeCacheMaxSize)
    {
        this.stripeCacheEnabled = stripeCacheEnabled;
        this.stripeCacheMode = requireNonNull(stripeCacheMode, "stripeCacheMode is null");
        this.stripeCacheMaxSize = requireNonNull(stripeCacheMaxSize, "stripeCacheMaxSize is null");
    }

    public boolean isStripeCacheEnabled()
    {
        return stripeCacheEnabled;
    }

    public DwrfStripeCacheMode getStripeCacheMode()
    {
        return stripeCacheMode;
    }

    public DataSize getStripeCacheMaxSize()
    {
        return stripeCacheMaxSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stripeCacheEnabled", stripeCacheEnabled)
                .add("stripeCacheMode", stripeCacheMode)
                .add("stripeCacheMaxSize", stripeCacheMaxSize)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean stripeCacheEnabled;
        private DwrfStripeCacheMode stripeCacheMode = DEFAULT_STRIPE_CACHE_MODE;
        private DataSize stripeCacheMaxSize = DEFAULT_STRIPE_CACHE_MAX_SIZE;

        public Builder withStripeCacheEnabled(boolean stripeCacheEnabled)
        {
            this.stripeCacheEnabled = stripeCacheEnabled;
            return this;
        }

        public Builder withStripeCacheMode(DwrfStripeCacheMode stripeCacheMode)
        {
            this.stripeCacheMode = requireNonNull(stripeCacheMode, "stripeCacheMode is null");
            return this;
        }

        public Builder withStripeCacheMaxSize(DataSize stripeCacheMaxSize)
        {
            this.stripeCacheMaxSize = requireNonNull(stripeCacheMaxSize, "stripeCacheMaxSize is null");
            return this;
        }

        public DwrfWriterOptions build()
        {
            return new DwrfWriterOptions(
                    stripeCacheEnabled,
                    stripeCacheMode,
                    stripeCacheMaxSize);
        }
    }
}
