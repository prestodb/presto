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

import com.facebook.presto.orc.metadata.StripeInformation;
import io.airlift.slice.Slice;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

class StripeInfo
{
    private final StripeInformation stripe;
    private final Optional<Slice> indexCache;
    private final Optional<Slice> footerCache;

    public StripeInfo(StripeInformation stripe)
    {
        this(stripe, Optional.empty(), Optional.empty());
    }

    public StripeInfo(StripeInformation stripe, Optional<Slice> indexCache, Optional<Slice> footerCache)
    {
        this.stripe = requireNonNull(stripe, "stripe is null");
        this.indexCache = requireNonNull(indexCache, "indexCache is null");
        this.footerCache = requireNonNull(footerCache, "footerCache is null");
    }

    public StripeInformation getStripe()
    {
        return stripe;
    }

    public Optional<Slice> getIndexCache()
    {
        return indexCache;
    }

    public Optional<Slice> getFooterCache()
    {
        return footerCache;
    }
}
