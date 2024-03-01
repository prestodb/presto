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

/**
 * Describes content of the DWRF stripe metadata cache.
 */
public enum DwrfStripeCacheMode
{
    // no cache
    NONE(false, false),

    // cache contains stripe indexes
    INDEX(true, false),

    // cache contains stripe footers
    FOOTER(false, true),

    // cache contains stripe indexes and footers
    INDEX_AND_FOOTER(true, true);

    private final boolean hasIndex;
    private final boolean hasFooter;

    DwrfStripeCacheMode(boolean hasIndex, boolean hasFooter)
    {
        this.hasIndex = hasIndex;
        this.hasFooter = hasFooter;
    }

    public boolean hasIndex()
    {
        return hasIndex;
    }

    public boolean hasFooter()
    {
        return hasFooter;
    }

    /**
     * Get the position of an element in the cacheOffsets that can be used to
     * get the offset for the index streams slice for a given stripe.
     */
    public int getIndexOffsetPosition(int stripeOrdinal)
    {
        switch (this) {
            case INDEX:
                return stripeOrdinal;
            case INDEX_AND_FOOTER:
                return stripeOrdinal * 2;
            default:
                throw new IllegalStateException("This mode doesn't support index cache: " + this);
        }
    }

    /**
     * Get the position of an element in the cacheOffsets that can be used to
     * get the offset for the stripe footer slice for a given stripe.
     */
    public int getFooterOffsetPosition(int stripeOrdinal)
    {
        switch (this) {
            case FOOTER:
                return stripeOrdinal;
            case INDEX_AND_FOOTER:
                return stripeOrdinal * 2 + 1;
            default:
                throw new IllegalStateException("This mode doesn't support footer cache: " + this);
        }
    }
}
