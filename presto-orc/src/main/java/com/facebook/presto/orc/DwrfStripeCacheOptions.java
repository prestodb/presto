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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DwrfStripeCacheOptions
{
    private final DwrfStripeCacheMode stripeCacheMode;
    private final DataSize stripeCacheMaxSize;

    public DwrfStripeCacheOptions(
            DwrfStripeCacheMode stripeCacheMode,
            DataSize stripeCacheMaxSize)
    {
        this.stripeCacheMode = requireNonNull(stripeCacheMode, "stripeCacheMode is null");
        this.stripeCacheMaxSize = requireNonNull(stripeCacheMaxSize, "stripeCacheMaxSize is null");
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
                .add("stripeCacheMode", stripeCacheMode)
                .add("stripeCacheMaxSize", stripeCacheMaxSize)
                .toString();
    }
}
