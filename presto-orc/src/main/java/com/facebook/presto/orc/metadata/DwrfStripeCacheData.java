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
package com.facebook.presto.orc.metadata;

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public class DwrfStripeCacheData
{
    private final Slice stripeCacheSlice;
    private final int stripeCacheSize;
    private final DwrfStripeCacheMode stripeCacheMode;

    public DwrfStripeCacheData(Slice stripeCacheSlice, int stripeCacheSize, DwrfStripeCacheMode stripeCacheMode)
    {
        this.stripeCacheSlice = requireNonNull(stripeCacheSlice, "stripeCacheSlice is null");
        this.stripeCacheSize = stripeCacheSize;
        this.stripeCacheMode = requireNonNull(stripeCacheMode, "stripeCacheMode is null");
    }

    public Slice getDwrfStripeCacheSlice()
    {
        return stripeCacheSlice;
    }

    public int getDwrfStripeCacheSize()
    {
        return stripeCacheSize;
    }

    public DwrfStripeCacheMode getDwrfStripeCacheMode()
    {
        return stripeCacheMode;
    }
}
