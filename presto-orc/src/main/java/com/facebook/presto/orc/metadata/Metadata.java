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

import com.facebook.presto.orc.metadata.statistics.StripeStatistics;

import java.util.List;
import java.util.Optional;

public class Metadata
{
    private final List<StripeStatistics> stripeStatistics;
    private final Optional<StripeMetaCache> stripeMetaCache;

    public Metadata(List<StripeStatistics> stripeStatistics)
    {
        this(stripeStatistics, Optional.empty());
    }

    public Metadata(List<StripeStatistics> stripeStatistics, Optional<StripeMetaCache> stripeMetaCache)
    {
        this.stripeStatistics = stripeStatistics;
        this.stripeMetaCache = stripeMetaCache;
    }

    public List<StripeStatistics> getStripeStatsList()
    {
        return stripeStatistics;
    }

    public Optional<StripeMetaCache> getStripeMetaCache()
    {
        return stripeMetaCache;
    }
}
