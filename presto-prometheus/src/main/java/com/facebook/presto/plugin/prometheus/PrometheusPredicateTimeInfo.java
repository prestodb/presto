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
package com.facebook.presto.plugin.prometheus;

import java.time.ZonedDateTime;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PrometheusPredicateTimeInfo
{
    private final Optional<ZonedDateTime> predicateLowerTimeBound;
    private final Optional<ZonedDateTime> predicateUpperTimeBound;

    private PrometheusPredicateTimeInfo(Optional<ZonedDateTime> predicateLowerTimeBound, Optional<ZonedDateTime> predicateUpperTimeBound)
    {
        this.predicateLowerTimeBound = requireNonNull(predicateLowerTimeBound, "predicateLowerTimeBound is null");
        this.predicateUpperTimeBound = requireNonNull(predicateUpperTimeBound, "predicateUpperTimeBound is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Optional<ZonedDateTime> getPredicateLowerTimeBound()
    {
        return predicateLowerTimeBound;
    }

    public Optional<ZonedDateTime> getPredicateUpperTimeBound()
    {
        return predicateUpperTimeBound;
    }

    public static final class Builder
    {
        private Optional<ZonedDateTime> predicateLowerTimeBound = Optional.empty();
        private Optional<ZonedDateTime> predicateUpperTimeBound = Optional.empty();

        private Builder() {}

        public void setPredicateLowerTimeBound(Optional<ZonedDateTime> predicateLowerTimeBound)
        {
            this.predicateLowerTimeBound = predicateLowerTimeBound;
        }

        public void setPredicateUpperTimeBound(Optional<ZonedDateTime> predicateUpperTimeBound)
        {
            this.predicateUpperTimeBound = predicateUpperTimeBound;
        }

        public PrometheusPredicateTimeInfo build()
        {
            return new PrometheusPredicateTimeInfo(predicateLowerTimeBound, predicateUpperTimeBound);
        }
    }
}
